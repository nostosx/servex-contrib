package endpoint_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// Claims is the principal produced by the token verifier.
type Claims struct{ Subject string }

func ExampleAuthentication() {
	verify := func(_ context.Context, token string) (Claims, error) {
		if token != "secret" {
			return Claims{}, errors.New("unknown token")
		}
		return Claims{Subject: "alice"}, nil
	}
	handler := func(ctx context.Context, _ any) (any, error) {
		c, _ := endpoint.PrincipalFromContext[Claims](ctx)
		return "hello " + c.Subject, nil
	}
	ep := endpoint.Authentication(verify)(handler)

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
