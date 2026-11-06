package client_test

import (
	"context"
	"fmt"

	clientmiddleware "github.com/nostosx/servex-contrib/middleware/client"
	"github.com/nostosx/servex/metadata"
)

func ExampleAuthentication() {
	mw := clientmiddleware.Authentication(func(context.Context) (string, error) { return "secret", nil })
	ep := mw(func(ctx context.Context, _ any) (any, error) {
		info, _ := metadata.FromContext(ctx)
		return info.Header.Get("authorization"), nil
	})

	// the contrib shared client puts a fresh outbound header into metadata.Info.
	ctx := metadata.NewContext(context.Background(), metadata.Info{Kind: metadata.KindClient, Header: metadata.MD{}})
	resp, _ := ep(ctx, nil)
	fmt.Println(resp)
	// Output: Bearer secret
}
