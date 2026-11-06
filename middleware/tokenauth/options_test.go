package tokenauth

import (
	"context"
	"strings"
	"testing"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

func TestStaticToken(t *testing.T) {
	for _, pair := range [][2]string{{"service-secret", "service-secret"}, {"service-secret", "wrong"}, {"", ""}, {"service-secret", ""}} {
		_, err := StaticToken(pair[0])(context.Background(), pair[1])
		valid := pair[0] != "" && pair[0] == pair[1]
		if valid && err != nil || !valid && errorx.CodeOf(err) != errorx.Unauthenticated {
			t.Fatal("incorrect verification outcome", err)
		}
		if err != nil && strings.Contains(err.Error(), "service-secret") {
			t.Fatal("credential leaked")
		}
	}
}

func TestTokenRejectsDuplicateHeaders(t *testing.T) {
	ctx := metadata.NewContext(t.Context(), metadata.Info{Header: metadata.Pairs("authorization", "Bearer one", "authorization", "Bearer two")})
	if _, err := NewOptions(nil).Token(ctx); errorx.CodeOf(err) != errorx.Unauthenticated {
		t.Fatal("ambiguous authorization accepted", err)
	}
}
