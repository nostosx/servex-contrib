// Package s3 adds bucket-scoped object operations using the official AWS SDK v2.
// Applications configure credentials, endpoints, retries and HTTP transport.
package s3

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Store is immutable. The application owns the SDK client and downloaded body
// streams. Operations preserve SDK errors, including smithy.APIError.
type Store struct {
	client    *sdk.Client
	presigner *sdk.PresignClient
	bucket    string
	prefix    string
}

// New restricts operations to a bucket and literal key prefix. Prefixes must end
// in '/' to isolate an application namespace. Object keys are not OS paths.
func New(client *sdk.Client, bucket, prefix string) (*Store, error) {
	if client == nil || strings.TrimSpace(bucket) == "" {
		return nil, errors.New("s3: client and bucket required")
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		return nil, errors.New("s3: prefix must end in /")
	}
	if len(prefix) > 1023 {
		return nil, errors.New("s3: prefix exceeds key limit")
	}
	return &Store{client: client, presigner: sdk.NewPresignClient(client), bucket: bucket, prefix: prefix}, nil
}

func (s *Store) key(key string) (string, error) {
	if key == "" || len(s.prefix)+len(key) > 1024 || strings.ContainsRune(key, 0) {
		return "", errors.New("s3: invalid object key")
	}
	return s.prefix + key, nil
}

// Put streams a single object with a known length. For large objects requiring
// multipart transfer, use the SDK transfer manager explicitly. The reader must
// honor cancellation where appropriate; a blocked custom reader cannot be stopped.
func (s *Store) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) (*sdk.PutObjectOutput, error) {
	name, err := s.key(key)
	if err != nil {
		return nil, err
	}
	if body == nil || size < 0 {
		return nil, errors.New("s3: body and non-negative length required")
	}
	return s.client.PutObject(ctx, &sdk.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(name), Body: body,
		ContentLength: aws.Int64(size), ContentType: aws.String(contentType),
	})
}

// Get returns the native response, including metadata and Body. Always close
// Body. The caller controls download limits; this adapter never buffers objects.
func (s *Store) Get(ctx context.Context, key string) (*sdk.GetObjectOutput, error) {
	name, err := s.key(key)
	if err != nil {
		return nil, err
	}
	return s.client.GetObject(ctx, &sdk.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(name)})
}

func (s *Store) Head(ctx context.Context, key string) (*sdk.HeadObjectOutput, error) {
	name, err := s.key(key)
	if err != nil {
		return nil, err
	}
	return s.client.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(name)})
}

func (s *Store) Delete(ctx context.Context, key string) error {
	name, err := s.key(key)
	if err != nil {
		return err
	}
	_, err = s.client.DeleteObject(ctx, &sdk.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(name)})
	return err
}

// List returns one bounded page. Prefix and returned object keys are relative
// to this Store's namespace, so a listed key can be passed to Get or Delete.
// Other metadata and continuation tokens retain their native SDK meaning.
func (s *Store) List(ctx context.Context, prefix, continuation string, limit int32) (*sdk.ListObjectsV2Output, error) {
	if limit < 1 || limit > 1000 || len(s.prefix)+len(prefix) > 1024 {
		return nil, errors.New("s3: invalid page size or prefix")
	}
	input := &sdk.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(s.prefix + prefix), MaxKeys: aws.Int32(limit),
	}
	if continuation != "" {
		input.ContinuationToken = aws.String(continuation)
	}
	output, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, err
	}
	page := *output
	page.Contents = slices.Clone(output.Contents)
	for i, object := range page.Contents {
		key, scoped := strings.CutPrefix(aws.ToString(object.Key), s.prefix)
		if !scoped {
			return nil, errors.New("s3: listing returned an object outside the configured namespace")
		}
		if _, err := s.key(key); err != nil {
			return nil, errors.New("s3: listing returned an invalid object key")
		}
		page.Contents[i].Key = aws.String(key)
	}
	return &page, nil
}

// PresignGet creates a bearer URL. Never log it or expose it beyond the intended
// recipient. Credential expiry may shorten the selected validity period.
func (s *Store) PresignGet(ctx context.Context, key string, ttl time.Duration) (*v4.PresignedHTTPRequest, error) {
	name, err := s.key(key)
	if err != nil {
		return nil, err
	}
	if err := validTTL(ttl); err != nil {
		return nil, err
	}
	return s.presigner.PresignGetObject(ctx,
		&sdk.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(name)}, sdk.WithPresignExpires(ttl))
}

// PresignPut returns required signed headers alongside the URL. It does not
// enforce an upload size policy; use an application-controlled upload flow when
// quotas or content validation must be checked before object storage accepts it.
func (s *Store) PresignPut(ctx context.Context, key, contentType string, ttl time.Duration) (*v4.PresignedHTTPRequest, error) {
	name, err := s.key(key)
	if err != nil {
		return nil, err
	}
	if err := validTTL(ttl); err != nil {
		return nil, err
	}
	return s.presigner.PresignPutObject(ctx, &sdk.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(name), ContentType: aws.String(contentType),
	}, sdk.WithPresignExpires(ttl))
}

func validTTL(ttl time.Duration) error {
	if ttl < time.Second || ttl > 7*24*time.Hour {
		return errors.New("s3: presign TTL must be between one second and seven days")
	}
	return nil
}
