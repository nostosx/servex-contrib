package connectx

import (
	"context"
	"net/http"

	"connectrpc.com/connect"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// UnaryMiddleware bridges servex middleware for generated or adapter handlers
// and clients. Middleware receives connect.AnyRequest and returns
// connect.AnyResponse; replacements must preserve their concrete message types.
// Streaming is deliberately separate and never automatically replayed.
func UnaryMiddleware(m ...endpoint.Middleware) connect.Interceptor {
	chain := endpoint.Chain(m...)
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			kind := metadata.KindServer
			if req.Spec().IsClient {
				kind = metadata.KindClient
			}
			ctx = callContext(ctx, kind, req.Peer().Protocol, req.Spec().Procedure, req.Header())
			ep := chain(func(ctx context.Context, in any) (any, error) {
				r, ok := in.(connect.AnyRequest)
				if !ok || r == nil {
					return nil, errorx.New(errorx.Internal, "RPC_MIDDLEWARE", "middleware replaced the RPC request with an invalid value")
				}
				if kind == metadata.KindClient {
					info, _ := metadata.FromContext(ctx)
					for key, values := range info.Header {
						r.Header().Del(key)
						for _, value := range values {
							r.Header().Add(key, value)
						}
					}
				}
				out, err := next(ctx, r)
				if kind == metadata.KindClient {
					return out, FromError(ctx, err)
				}
				return out, err
			})
			res, err := ep(ctx, req)
			if err != nil {
				if kind == metadata.KindClient {
					return nil, FromError(ctx, err)
				}
				return nil, ToError(err)
			}
			out, ok := res.(connect.AnyResponse)
			if !ok || out == nil {
				return nil, ToError(errorx.New(errorx.Internal, "RPC_MIDDLEWARE", "middleware returned an invalid RPC response"))
			}
			return out, nil
		}
	})
}

// StreamMiddleware wraps the entire server stream lifetime. The endpoint
// request is connect.StreamingHandlerConn, and its successful reply is nil.
// Install only middleware suitable for streams: retries are not safe here.
// Client streams remain owned by the SDK and are not intercepted by this API.
func StreamMiddleware(m ...endpoint.Middleware) connect.Interceptor {
	return streamInterceptor{chain: endpoint.Chain(m...)}
}

type streamInterceptor struct{ chain endpoint.Middleware }

func (s streamInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc { return next }
func (s streamInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}
func (s streamInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		ctx = callContext(ctx, metadata.KindServer, conn.Peer().Protocol, conn.Spec().Procedure, conn.RequestHeader())
		_, err := s.chain(func(ctx context.Context, _ any) (any, error) {
			return nil, next(ctx, conn)
		})(ctx, conn)
		return ToError(err)
	}
}

func callContext(ctx context.Context, kind metadata.Kind, protocol, operation string, header http.Header) context.Context {
	md := make(metadata.MD, len(header))
	for key, values := range header {
		for _, value := range values {
			md.Add(key, value)
		}
	}
	return metadata.NewContext(ctx, metadata.Info{Kind: kind, Protocol: protocol, Operation: operation, Header: md})
}
