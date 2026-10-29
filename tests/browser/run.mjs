import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { chromium, webkit } from "playwright";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../..");
const core = path.resolve(process.env.SERVEX_CORE ?? path.join(root, "../servex"));
const work = await mkdtemp(path.join(tmpdir(), "servex-browser-"));
const output = path.resolve(process.env.BROWSER_OUTPUT ?? path.join(here, "results"));
const go = process.env.GO ?? "go";
const env = { ...process.env, GOWORK: "off", GOENV: "off", GOTOOLCHAIN: "local" };
const engines = (process.env.BROWSERS ?? "chrome,webkit").split(",");
if (engines.some(engine => !["chrome", "webkit"].includes(engine)) || new Set(engines).size !== engines.length) {
  throw new Error("BROWSERS must select chrome, webkit, or chrome,webkit without duplicates");
}

function command(executable, args, cwd = here) {
  const result = spawnSync(executable, args, { cwd, env, encoding: "utf8" });
  if (result.status !== 0) throw new Error(`${executable} ${args.join(" ")} failed:\n${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

await mkdir(output, { recursive: true });
await mkdir(path.join(here, ".generated"), { recursive: true });
command(process.env.PROTOC ?? "protoc", ["--proto_path=.", `--plugin=protoc-gen-es=${path.join(here, "node_modules/.bin/protoc-gen-es")}`, "--es_out=.generated", "--es_opt=target=js", "browser.proto"]);
await build({ entryPoints: [path.join(here, "browser.js")], outfile: path.join(work, "browser.js"), bundle: true, platform: "browser", format: "iife" });

// Explicit source overrides stay in this disposable fixture module. Library
// manifests, public versions and the caller's Go configuration remain intact.
const moduleDirs = [core];
for (const capability of await readdir(root, { withFileTypes: true })) {
  if (!capability.isDirectory() || capability.name.startsWith(".")) continue;
  const capabilityDir = path.join(root, capability.name);
  try { await readFile(path.join(capabilityDir, "go.mod")); moduleDirs.push(capabilityDir); } catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const implementation of await readdir(path.join(root, capability.name), { withFileTypes: true })) {
    if (!implementation.isDirectory()) continue;
    const dir = path.join(root, capability.name, implementation.name);
    try { await readFile(path.join(dir, "go.mod")); moduleDirs.push(dir); } catch (error) { if (error.code !== "ENOENT") throw error; }
  }
}
let mod = "module servex-browser-fixture\n\ngo 1.27.3\n\n";
for (const dir of moduleDirs) {
  const source = await readFile(path.join(dir, "go.mod"), "utf8");
  const modulePath = /^module\s+(\S+)/m.exec(source)[1];
  mod += `replace ${modulePath} => ${JSON.stringify(dir)}\n`;
}
await writeFile(path.join(work, "go.mod"), mod);
const fixtureSource = await readFile(path.join(here, "fixture.go"), "utf8");
await writeFile(path.join(work, "main.go"), fixtureSource.replace("//go:build ignore\n\n", ""));
command(go, ["mod", "tidy"], work);
command(go, ["build", "-race", "-o", path.join(work, "fixture"), "main.go"], work);
const fixture = spawn(path.join(work, "fixture"), ["-assets", work], { cwd: work, env, stdio: ["ignore", "pipe", "pipe"] });
let stderr = "";
fixture.stderr.on("data", chunk => { stderr += chunk; });
const ready = await new Promise((resolve, reject) => {
  let text = "";
  const timer = setTimeout(() => reject(new Error("fixture did not start")), 15000);
  fixture.once("exit", code => { clearTimeout(timer); reject(new Error(`fixture exited ${code}: ${stderr}`)); });
  fixture.stdout.on("data", chunk => {
    text += chunk;
    if (!text.includes("\n")) return;
    clearTimeout(timer);
    try { resolve(JSON.parse(text.slice(0, text.indexOf("\n")))); } catch (error) { reject(error); }
  });
}).catch(error => { fixture.kill("SIGTERM"); throw error; });
console.log(JSON.stringify({ fixture: ready, temporaryModule: work }));

const reports = [];
try {
  for (const engine of engines) {
    const browser = await (engine === "webkit" ? webkit.launch({ headless: true }) : chromium.launch({ channel: "chrome", headless: true }));
    const context = await browser.newContext();
    const consoleMessages = [];
    const page = await context.newPage();
    let phase = "startup";
    page.on("console", message => consoleMessages.push({ phase, type: message.type(), text: message.text() }));
    page.on("pageerror", error => consoleMessages.push({ phase, type: "pageerror", text: String(error) }));
    const report = { engine, version: browser.version(), results: [], console: consoleMessages };
    try {
      for (const origin of ["same", "allowed", "denied"]) {
        phase = origin;
        await page.goto(origin === "same" ? ready.api : ready[origin]);
        await page.waitForFunction(() => typeof window.runSuite === "function");
        report.userAgent = await page.evaluate(() => navigator.userAgent);
        report.results.push(...await page.evaluate(async ({ config, origin }) => {
          return origin === "denied" ? window.runDenied(config) : window.runSuite({ ...config, origin });
        }, { config: ready, origin }));
      }
      await page.evaluate(results => { document.querySelector("#results").textContent = JSON.stringify(results, null, 2); }, report.results);
      await page.screenshot({ path: path.join(output, `${engine}.png`), fullPage: true });
    } finally { await browser.close(); }
    reports.push(report);
    const failed = report.results.filter(result => result.status !== "passed");
    console.log(`${engine} ${report.version}: ${report.results.length - failed.length}/${report.results.length} passed`);
    for (const result of failed) console.log(`${result.name}: ${result.error}`);
    await writeFile(path.join(output, `${engine}.json`), JSON.stringify(report, null, 2) + "\n");
  }
} finally {
  const exited = fixture.exitCode !== null || fixture.signalCode !== null ? Promise.resolve() : new Promise(resolve => fixture.once("exit", resolve));
  fixture.kill("SIGTERM");
  await exited;
  await writeFile(path.join(output, "fixture.log"), stderr);
}
await writeFile(path.join(output, "report.json"), JSON.stringify(reports, null, 2) + "\n");
const unexpectedPageErrors = reports.flatMap(report => report.console).filter(message => {
  return message.type === "pageerror" && !(message.phase === "denied" && /access control|cross-origin|cors/i.test(message.text));
});
if (reports.some(report => report.results.some(result => result.status !== "passed")) || unexpectedPageErrors.length > 0 || /DATA RACE/.test(stderr)) process.exitCode = 1;
