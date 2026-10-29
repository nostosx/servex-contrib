import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { createConnectTransport, createGrpcWebTransport } from "@connectrpc/connect-web";
import { BrowserService } from "./.generated/browser_pb.js";

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function within(promise, milliseconds = 15000) {
  let timer;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("browser operation timed out")), milliseconds);
    })]);
  } finally {
    clearTimeout(timer);
  }
}

async function waitFor(check) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  throw new Error("server did not observe expected lifecycle state");
}

const unique = () => crypto.randomUUID();
const state = async (base, id) => {
  const response = await fetch(`${base}/state?id=${id}`);
  assert(response.ok, `state endpoint returned ${response.status}`);
  return response.json();
};
const release = async (base, id) => {
  const response = await fetch(`${base}/release?id=${id}`, { method: "POST" });
  assert(response.ok, `release endpoint returned ${response.status}`);
};

async function sseLive(base) {
  const id = unique();
  let source;
  try {
    await within(new Promise((resolve, reject) => {
      source = new EventSource(`${base}/events?id=${id}`, { withCredentials: true });
      source.onerror = () => reject(new Error("EventSource failed before completion"));
      source.addEventListener("token", async event => {
        try {
          assert(event.data === "hello\n世界", "SSE multiline/UTF-8 data changed");
          assert(event.lastEventId === "cursor-1", "SSE event ID missing");
          assert(!(await state(base, id)).finished, "SSE was buffered until handler completion");
          await release(base, id);
        } catch (error) { reject(error); }
      });
      source.onmessage = event => {
        try {
          assert(event.data === "done", "SSE final message missing");
          assert(event.lastEventId === "", "empty SSE ID did not reset the browser cursor");
          source.close();
          resolve();
        } catch (error) { reject(error); }
      };
    }));
  } finally { source?.close(); }
}

async function sseResume(base) {
  const id = unique();
  const values = [];
  let source;
  try {
    await within(new Promise((resolve, reject) => {
      source = new EventSource(`${base}/events?mode=resume&id=${id}`);
      source.addEventListener("token", event => {
        values.push(event.data);
        if (event.data === "resumed") { source.close(); resolve(); }
        if (values.length > 2) reject(new Error("EventSource did not carry Last-Event-ID"));
      });
      // A dropped connection is expected here: native EventSource reconnects.
    }));
    assert(values.join(",") === "initial,resumed", `unexpected replay: ${values}`);
    const observed = await state(base, id);
    assert(observed.requests === 2 && observed.lastId === "cursor-1", "browser resume header was lost");
  } finally { source?.close(); }
}

async function sseCancel(base) {
  const id = unique();
  let source;
  try {
    await within(new Promise((resolve, reject) => {
      source = new EventSource(`${base}/events?id=${id}`);
      source.addEventListener("token", () => { source.close(); resolve(); });
      source.onerror = () => reject(new Error("EventSource failed before cancel"));
    }));
    await waitFor(async () => (await state(base, id)).canceled);
  } finally { source?.close(); }
}

async function websocketEcho(base) {
  let socket;
  try {
    await within(new Promise((resolve, reject) => {
      socket = new WebSocket(`${base.replace("http", "ws")}/ws`, "echo");
      socket.binaryType = "arraybuffer";
      let received = 0;
      socket.onopen = () => {
        try {
          assert(socket.protocol === "echo", "WebSocket subprotocol negotiation failed");
          socket.send("hello 世界");
        } catch (error) { reject(error); }
      };
      socket.onmessage = event => {
        try {
          if (received++ === 0) {
            assert(event.data === "hello 世界", "WebSocket text changed");
            socket.send(new Uint8Array([0, 1, 254, 255]));
          } else {
            assert(Array.from(new Uint8Array(event.data)).join(",") === "0,1,254,255", "WebSocket binary changed");
            socket.close(1000, "browser finished");
          }
        } catch (error) { reject(error); }
      };
      socket.onerror = () => reject(new Error("WebSocket handshake/message error"));
      socket.onclose = event => {
        try {
          assert(received === 2 && event.code === 1000 && event.wasClean, `unclean close: ${event.code}`);
          resolve();
        } catch (error) { reject(error); }
      };
    }));
  } finally {
    if (socket && socket.readyState < WebSocket.CLOSING) socket.close();
  }
}

function rpcClient(base, protocol, binary) {
  const options = { baseUrl: base, useBinaryFormat: binary };
  const transport = protocol === "connect" ? createConnectTransport(options) : createGrpcWebTransport(options);
  return createClient(BrowserService, transport);
}

async function rpcUnary(client) {
  let header, trailer;
  const response = await client.unary({ value: "hello 世界" }, {
    headers: { "x-case-id": unique() },
    onHeader: h => { header = h.get("x-reply"); },
    onTrailer: h => { trailer = h.get("x-end"); },
  });
  assert(response.value === "hello 世界", "RPC message changed");
  assert(header === "browser" && trailer === "done", `RPC headers/trailers missing: ${header}/${trailer}`);
  let failure;
  try { await client.unary({ value: "error" }); } catch (error) { failure = error; }
  assert(failure instanceof ConnectError && failure.code === Code.Unavailable, `RPC status lost: ${failure}`);
  assert(failure.details.length > 0, "RPC ErrorInfo detail missing");
}

async function rpcStream(base, client) {
  const id = unique();
  const values = [];
  let header, trailer;
  for await (const message of client.watch({ value: "live" }, {
    headers: { "x-case-id": id },
    onHeader: h => { header = h.get("x-reply"); },
    onTrailer: h => { trailer = h.get("x-end"); },
  })) {
    values.push(message.value);
    if (values.length === 1) {
      assert(!(await state(base, id)).finished, "RPC stream buffered until completion");
      await release(base, id);
    }
  }
  assert(values.join(",") === "first,second", `stream values: ${values}`);
  assert(header === "stream" && trailer === "done", `stream metadata: ${header}/${trailer}`);
}

async function rpcCancel(base, client, earlyReturn) {
  const id = unique();
  const abort = new AbortController();
  let count = 0, failure;
  try {
    for await (const message of client.watch({ value: "cancel" }, {
      signal: abort.signal, headers: { "x-case-id": id },
    })) {
      assert(message.value === "first", "stream did not start");
      count++;
      if (earlyReturn) break;
      abort.abort();
    }
  } catch (error) { failure = error; }
  finally { abort.abort(); }
  assert(count === 1, "canceled stream did not deliver exactly one message");
  if (!earlyReturn) assert(failure instanceof ConnectError && failure.code === Code.Canceled, `cancel status: ${failure}`);
  await waitFor(async () => (await state(base, id)).canceled);
}

async function rpcStreamError(client) {
  const values = [];
  let failure;
  try {
    for await (const message of client.watch({ value: "error" }, { headers: { "x-case-id": unique() } })) values.push(message.value);
  } catch (error) { failure = error; }
  assert(values.join(",") === "first", "partial response lost");
  assert(failure instanceof ConnectError && failure.code === Code.ResourceExhausted, `stream error lost: ${failure}`);
}

async function unsupportedStreams(client) {
  async function* input() { yield { value: "test" }; }
  for (const method of ["upload", "chat"]) {
    let failure;
    try {
      if (method === "upload") await client.upload(input());
      else for await (const _ of client.chat(input())) { /* unsupported in browser transports */ }
    } catch (error) { failure = error; }
    assert(failure, `browser unexpectedly accepted ${method} streaming`);
    assert(String(failure).toLowerCase().includes("streaming request"), `unexpected unsupported-stream error: ${failure}`);
  }
}

async function record(results, name, run) {
  const started = performance.now();
  try {
    await within(run());
    results.push({ name, status: "passed", milliseconds: Math.round(performance.now() - started) });
  } catch (error) {
    results.push({ name, status: "failed", error: String(error?.stack ?? error) });
  }
  document.querySelector("#results").textContent = JSON.stringify(results, null, 2);
}

window.runSuite = async ({ api, origin }) => {
  const results = [];
  for (const [name, run] of [["sse.live", sseLive], ["sse.resume", sseResume], ["sse.cancel", sseCancel], ["websocket.echo-close", websocketEcho]]) {
    await record(results, `${origin}.${name}`, () => run(api));
  }
  for (const [protocol, binary] of [["connect", true], ["connect", false], ["grpc-web", true]]) {
    const client = rpcClient(api, protocol, binary);
    const prefix = `${origin}.${protocol}.${binary ? "protobuf" : "json"}`;
    for (const [name, run] of [
      ["unary-errors-metadata", () => rpcUnary(client)],
      ["stream-metadata", () => rpcStream(api, client)],
      ["abort", () => rpcCancel(api, client, false)],
      ["iterator-return", () => rpcCancel(api, client, true)],
      ["stream-error", () => rpcStreamError(client)],
      ["unsupported-request-streams", () => unsupportedStreams(client)],
    ]) await record(results, `${prefix}.${name}`, run);
  }
  return results;
};

window.runDenied = async ({ api }) => {
  const results = [];
  await record(results, "denied.sse", async () => {
    let source;
    try {
      await within(new Promise((resolve, reject) => {
        source = new EventSource(`${api}/events?id=${unique()}`);
        source.onopen = () => reject(new Error("CORS accepted denied origin"));
        source.onerror = () => resolve();
      }));
    } finally { source?.close(); }
  });
  await record(results, "denied.websocket", async () => {
    const socket = new WebSocket(`${api.replace("http", "ws")}/ws`);
    try {
      await within(new Promise((resolve, reject) => {
        socket.onopen = () => reject(new Error("Origin policy accepted denied WebSocket"));
        socket.onerror = () => resolve();
      }));
    } finally { if (socket.readyState < WebSocket.CLOSING) socket.close(); }
  });
  for (const protocol of ["connect", "grpc-web"]) {
    await record(results, `denied.${protocol}`, async () => {
      let failure;
      try { await rpcClient(api, protocol, true).unary({ value: "denied" }); } catch (error) { failure = error; }
      assert(failure, "CORS accepted denied RPC origin");
    });
  }
  return results;
};
