package grpcx

import (
	"context"

	grpcmd "google.golang.org/grpc/metadata"

	"github.com/nostosx/servex/metadata"
)

// incomingMD converts the incoming gRPC metadata of ctx into metadata.MD.
// The result is never nil.
func incomingMD(ctx context.Context) metadata.MD {
	in, _ := grpcmd.FromIncomingContext(ctx)
	md := make(metadata.MD, len(in))
	for k, vs := range in {
		for _, v := range vs {
			md.Add(k, v)
		}
	}
	return md
}

// withOutgoingMD appends header to the outgoing gRPC metadata of ctx.
func withOutgoingMD(ctx context.Context, header metadata.MD) context.Context {
	if len(header) == 0 {
		return ctx
	}
	out, _ := grpcmd.FromOutgoingContext(ctx)
	out = out.Copy()
	for k, vs := range header.All() {
		out.Append(k, vs...)
	}
	return grpcmd.NewOutgoingContext(ctx, out)
}
