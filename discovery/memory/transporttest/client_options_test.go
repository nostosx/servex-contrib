package transport_test

import (
	"context"

	sxclient "github.com/nostosx/servex-contrib/transport/client"

	"github.com/nostosx/servex-contrib/selector/random"
)

func newClient(ctx context.Context, target string, opts ...sxclient.Option) (*sxclient.Client, error) {
	return sxclient.New(ctx, target, append([]sxclient.Option{sxclient.WithSelector(random.New())}, opts...)...)
}
