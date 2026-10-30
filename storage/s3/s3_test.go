package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func sdkClient(endpoint string) *sdk.Client {
	return sdk.New(sdk.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		HTTPClient: &http.Client{Timeout: 5 * time.Second}, RetryMaxAttempts: 1,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "servex-test", SecretAccessKey: "servex-test-secret"}, nil
		}),
	})
}

// protocolFixture exercises actual AWS serialization, HTTP transport, errors and
// presigning. It is not acceptance against AWS or every S3-compatible backend.
func protocolFixture(t *testing.T) *sdk.Client {
	t.Helper()
	var mu sync.Mutex
	objects := make(map[string][]byte)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		if r.URL.Query().Get("list-type") == "2" {
			if r.URL.Query().Get("prefix") != "app/" || r.URL.Query().Get("max-keys") != "1" {
				t.Error(r.URL)
			}
			w.Header().Set("Content-Type", "application/xml")
			if continuation := r.URL.Query().Get("continuation-token"); continuation != "" {
				if continuation != "next" {
					t.Error("unexpected continuation", continuation)
				}
				_, _ = io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>app/second</Key><Size>6</Size></Contents></ListBucketResult>`)
				return
			}
			_, _ = io.WriteString(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>app/file</Key><Size>7</Size></Contents></ListBucketResult>`)
			return
		}
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			objects[key] = body
			w.Header().Set("ETag", `"etag"`)
		case http.MethodGet, http.MethodHead:
			body, ok := objects[key]
			if !ok {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			if r.Method == http.MethodGet {
				_, _ = w.Write(body)
			}
		case http.MethodDelete:
			delete(objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Error("unexpected method", r.Method)
		}
	}))
	t.Cleanup(server.Close)
	return sdkClient(server.URL)
}

func TestListPreservesPaginationAndMetadata(t *testing.T) {
	store, err := New(protocolFixture(t), "bucket", "app/")
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.List(t.Context(), "", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !aws.ToBool(first.IsTruncated) || aws.ToString(first.NextContinuationToken) != "next" {
		t.Fatalf("pagination metadata lost: %+v", first)
	}
	second, err := store.List(t.Context(), "", aws.ToString(first.NextContinuationToken), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Contents) != 1 {
		t.Fatal(second)
	}
	object := second.Contents[0]
	if aws.ToString(object.Key) != "second" || aws.ToInt64(object.Size) != 6 || aws.ToBool(second.IsTruncated) {
		t.Fatalf("second page lost key or metadata: %+v", second)
	}
}

func exercise(t *testing.T, store *Store) {
	t.Helper()
	ctx := t.Context()
	if _, err := store.Put(ctx, "file", strings.NewReader("content"), 7, "text/plain"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.Delete(ctx, "file"); err != nil {
			t.Error(err)
		}
	})
	output, err := store.Get(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(output.Body)
	if closeErr := output.Body.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || string(body) != "content" {
		t.Fatalf("body=%s err=%v", body, err)
	}
	info, err := store.Head(ctx, "file")
	if err != nil || aws.ToInt64(info.ContentLength) != 7 {
		t.Fatalf("head=%+v err=%v", info, err)
	}
	page, err := store.List(ctx, "", "", 1)
	if err != nil || len(page.Contents) != 1 {
		t.Fatalf("list=%+v err=%v", page, err)
	}
	listed := page.Contents[0]
	if aws.ToString(listed.Key) != "file" || aws.ToInt64(listed.Size) != 7 {
		t.Fatalf("listed key or metadata changed: %+v", listed)
	}
	listedObject, err := store.Get(ctx, aws.ToString(listed.Key))
	if err != nil {
		t.Fatalf("listed key cannot be read: %v", err)
	}
	if err := listedObject.Body.Close(); err != nil {
		t.Fatal(err)
	}
	get, err := store.PresignGet(ctx, "file", time.Minute)
	if err != nil || !strings.Contains(get.URL, "X-Amz-Signature=") {
		t.Fatalf("presign=%+v err=%v", get, err)
	}
	put, err := store.PresignPut(ctx, "other", "text/plain", time.Minute)
	if err != nil || put.Method != http.MethodPut {
		t.Fatalf("presign PUT=%+v err=%v", put, err)
	}
	if err := store.Delete(ctx, aws.ToString(listed.Key)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "file"); err == nil {
		t.Fatal("deleted object readable")
	}
}

func TestListRejectsInvalidBackendKeys(t *testing.T) {
	for _, test := range []struct {
		name string
		key  string
	}{
		{name: "outside namespace", key: "other/file"},
		{name: "empty relative key", key: "app/"},
		{name: "absent key", key: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				_, _ = io.WriteString(w, "<ListBucketResult><Contents><Key>"+test.key+"</Key></Contents></ListBucketResult>")
			}))
			defer server.Close()
			store, err := New(sdkClient(server.URL), "bucket", "app/")
			if err != nil {
				t.Fatal(err)
			}
			if page, err := store.List(t.Context(), "", "", 1); err == nil || page != nil {
				t.Fatalf("invalid listing accepted: page=%+v err=%v", page, err)
			}
		})
	}
}

func TestAWSProtocolContract(t *testing.T) {
	store, err := New(protocolFixture(t), "bucket", "app/")
	if err != nil {
		t.Fatal(err)
	}
	exercise(t, store)
	_, err = store.Get(t.Context(), "missing")
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "NoSuchKey" {
		t.Fatalf("lost SDK error: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.Get(ctx, "file"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := store.PresignGet(t.Context(), "file", 8*24*time.Hour); err == nil {
		t.Fatal("excessive presign TTL")
	}
	if _, err := store.List(t.Context(), "", "", 1001); err == nil {
		t.Fatal("unbounded page")
	}
	if _, err := New(nil, "bucket", ""); err == nil {
		t.Fatal("nil client")
	}
}

// Keys remain literal S3 names and are scoped to the configured namespace.
func TestLiteralNamespace(t *testing.T) {
	store, _ := New(sdkClient("http://127.0.0.1:1"), "bucket", "app/")
	key, err := store.key("folder/a b")
	if err != nil || key != "app/folder/a b" {
		t.Fatal(key, err)
	}
	if _, err := store.key(""); err == nil {
		t.Fatal("empty key")
	}
}
