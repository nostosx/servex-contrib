package p2c_test

import (
	"context"
	"fmt"
	"time"

	"github.com/nostosx/servex-contrib/selector/p2c"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/selector"
)

func Example() {
	sel := p2c.New()
	nodes := []selector.Node{{Addr: "a:80"}, {Addr: "b:80"}}
	ctx := context.Background()

	// Report a failure of b: it is penalized and a is preferred afterwards.
	for {
		n, done, _ := sel.Select(ctx, nodes)
		if n.Addr == "b:80" {
			done(ctx, errorx.New(errorx.Unavailable, "DOWN", "connection refused"))
			break
		}
		time.Sleep(time.Millisecond)
		done(ctx, nil)
	}
	n, done, _ := sel.Select(ctx, nodes)
	done(ctx, nil)
	fmt.Println(n.Addr)
	// Output: a:80
}
