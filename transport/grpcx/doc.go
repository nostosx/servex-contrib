// Package grpcx implements the servex transport contracts on top of
// google.golang.org/grpc: Server runs gRPC services as an app.Hook with the
// servex endpoint middleware, and Client exposes a grpc.ClientConnInterface
// whose calls go through a transport.Client for middleware, service
// discovery and node selection.
// Unary and streaming middleware are configured independently. Stream chains
// receive *StreamInfo and cover the full RPC, with no automatic stream replay.
//
// Errors are mapped between errorx and gRPC statuses. On the server, an
// *errorx.Error becomes a status with the same numeric code and message; its
// Reason and Metadata travel in a google.rpc.ErrorInfo detail. Errors that
// already carry a gRPC status are sent unchanged, and other errors are
// classified with errorx.From. On the client, every failed call yields an
// *errorx.Error restored from the status. Its retryability follows the code
// unless the server sent an explicit decision under RetryableMetadataKey; the
// original status error stays in the cause chain, so status.FromError keeps
// working. Restored errors are marked remote (errorx.WithRemote), so a server
// returning them sends errorx.Error.PropagatedRetryable and its caller does
// not retry a failure the receiving hop may already have retried.
package grpcx
