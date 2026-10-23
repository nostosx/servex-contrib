package auth_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/nostosx/servex-contrib/middleware/auth"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// Claims is the principal produced by the token verifier.
type Claims struct{ Subject string }

func Example() {
	verify := func(_ context.Context, token string) (Claims, error) {
		if token != "secret" {
			return Claims{}, errors.New("unknown token")
		}
		return Claims{Subject: "alice"}, nil
	}
	handler := func(ctx context.Context, _ any) (any, error) {
		c, _ := auth.FromContext[Claims](ctx)
		return "hello " + c.Subject, nil
	}
	ep := auth.New(verify)(handler)

	// A transport puts the inbound header into metadata.Info.
	ctx := metadata.NewContext(context.Background(), metadata.Info{
		Kind:   metadata.KindServer,
		Header: metadata.Pairs("Authorization", "Bearer secret"),
	})
	resp, _ := ep(ctx, nil)
	fmt.Println(resp)

	_, err := ep(context.Background(), nil)
	fmt.Println(errorx.CodeOf(err))
	// Output:
	// hello alice
	// Unauthenticated
}

func ExampleClient() {
	mw := auth.Client(func(context.Context) (string, error) { return "secret", nil })
	ep := mw(func(ctx context.Context, _ any) (any, error) {
		info, _ := metadata.FromContext(ctx)
		return info.Header.Get("authorization"), nil
	})

	// transport.Client puts a fresh outbound header into metadata.Info.
	ctx := metadata.NewContext(context.Background(), metadata.Info{Kind: metadata.KindClient, Header: metadata.MD{}})
	resp, _ := ep(ctx, nil)
	fmt.Println(resp)
	// Output: Bearer secret
}
