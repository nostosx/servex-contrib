# s3

**Unreleased.** General object storage using the official AWS SDK for Go v2:
service/s3 v1.114.6, aws v1.47.5. Go 1.27.3; MIT. This is independent of
agentx-contrib/s3, which loads RAG documents rather than providing object CRUD.

## Usage

This complete program writes and reads a demo object under a fixed application
prefix. Supply S3_BUCKET, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. S3_ENDPOINT
is optional for a compatible service; select the correct region for AWS.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strings"
    "time"
    "log"

    "github.com/aws/aws-sdk-go-v2/aws"
    sdk "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/nostosx/servex-contrib/storage/s3"
)

func run() error {
    options := sdk.Options{Region:"us-east-1", HTTPClient:&http.Client{Timeout:30*time.Second},
        Credentials:aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
            return aws.Credentials{AccessKeyID:os.Getenv("AWS_ACCESS_KEY_ID"),
                SecretAccessKey:os.Getenv("AWS_SECRET_ACCESS_KEY"),
                SessionToken:os.Getenv("AWS_SESSION_TOKEN")}, nil
        })}
    if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
        options.BaseEndpoint = aws.String(endpoint)
        options.UsePathStyle = true
    }
    store, err := s3.New(sdk.New(options), os.Getenv("S3_BUCKET"), "demo/")
    if err != nil { return err }
    ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
    defer cancel()
    if _, err := store.Put(ctx, "hello.txt", strings.NewReader("hello"), 5, "text/plain"); err != nil { return err }
    object, err := store.Get(ctx, "hello.txt")
    if err != nil { return err }
    defer object.Body.Close()
    body, err := io.ReadAll(io.LimitReader(object.Body, 1024))
    if err != nil { return err }
    fmt.Println(string(body))
    return nil
}

func main() { if err := run(); err != nil { log.Fatal(err) } }
```

Use a managed SDK credentials provider for production when appropriate; no
credential discovery or account provisioning is performed by this adapter.

## Contract

Put, Get, Head and Delete operate in one configured bucket/key prefix. Get returns
the SDK body stream; always close it. Put is a single-object transfer with a known
length, not a multipart upload manager. SDK errors retain their original identity.
Keys are literal S3 names, not filesystem paths. Prefixes must end in '/'; this
scoping convenience is not a substitute for bucket IAM or tenant authorization.

List accepts a page size of 1..1000 and preserves the native continuation token.
Returned Contents keys are relative to the configured namespace, matching Get,
Head and Delete. Other SDK metadata is retained. A listed key can be used directly:

```go
page, err := store.List(ctx, "", "", 100)
if err != nil { return err }
for _, object := range page.Contents {
    response, err := store.Get(ctx, aws.ToString(object.Key))
    if err != nil { return err }
    // Consume the response within the application's download limit.
    if err := response.Body.Close(); err != nil { return err }
}
```

This is an assembly fragment using the store/context above. Backend results with
missing, invalid or out-of-namespace keys are rejected. No unbounded listing or
object buffering occurs. This normalized-key behavior belongs to the unreleased
adapter API; advanced raw SDK calls continue to use full bucket keys.

PresignGet/PresignPut return the SDK URL, method and signed headers. TTL is one
second through seven days; expiring credentials may shorten it. Send required
signed headers. A URL is a bearer credential: do not log it. Presigned PUT does
not impose an upload-size or content-validation policy. Operations are
unconditional; use native SDK APIs explicitly for advanced conditional/versioned
operations. Client transport, TLS, retries and connection shutdown are caller-owned.

## Checks

`task check lint vulncheck MODULES=storage/s3` from the root. Unit tests use actual
SDK serialization/signing against a local HTTP protocol fixture. The real-service
gate creates a uniquely named bucket, verifies CRUD/listing including reading and
deleting a listed relative key, and uses independent
HTTP requests for presigned uploads/downloads, then deletes its objects and bucket:

```sh
SERVEX_IT_S3_ENDPOINT=http://127.0.0.1:9000 task test INTEGRATION=true MODULES=storage/s3
```

The disposable fixture uses access key servex-test and its test-only secret from
.github/scripts/start-service.sh. CI uses a pinned MinIO fixture. This proves that
backend's compatibility, not live AWS credentials or every S3-compatible service.
