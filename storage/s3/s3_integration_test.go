//go:build integration

package s3

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestIntegrationS3(t *testing.T) {
	endpoint := os.Getenv("SERVEX_IT_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("SERVEX_IT_S3_ENDPOINT not set")
	}
	client := sdkClient(endpoint)
	bucket := "servex-it-" + uuid.NewV7().String()
	if _, err := client.CreateBucket(t.Context(), &sdk.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.DeleteObject(ctx, &sdk.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("app/other")})
		if _, err := client.DeleteBucket(ctx, &sdk.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Error(err)
		}
	})
	store, err := New(client, bucket, "app/")
	if err != nil {
		t.Fatal(err)
	}
	exercise(t, store)
	// Exercise the signed URLs with an independent HTTP client, including PUT's
	// signed content type; successful local signing alone proves no server access.
	put, err := store.PresignPut(t.Context(), "other", "text/plain", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, put.URL, strings.NewReader("signed"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header = put.SignedHeader.Clone()
	request.Header.Set("Content-Type", "text/plain")
	httpClient := &http.Client{Timeout: 5 * time.Second}
	response, err := httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode/100 != 2 {
		t.Fatal("presigned upload", response.Status)
	}
	get, err := store.PresignGet(t.Context(), "other", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, get.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	if closeErr := response.Body.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || response.StatusCode != http.StatusOK || string(body) != "signed" {
		t.Fatalf("signed download=%s status=%d err=%v", body, response.StatusCode, err)
	}
}
