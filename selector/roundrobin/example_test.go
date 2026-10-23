package roundrobin_test

import (
	"context"
	"fmt"

	"github.com/nostosx/servex-contrib/selector/roundrobin"
	"github.com/nostosx/servex/selector"
)

func Example() {
	sel := roundrobin.New()
	nodes := []selector.Node{
		{Addr: "a:80", Weight: 5},
		{Addr: "b:80", Weight: 1},
		{Addr: "c:80", Weight: 1},
	}
	for range 7 {
		n, done, _ := sel.Select(context.Background(), nodes)
		done(context.Background(), nil)
		fmt.Print(n.Addr[:1], " ")
	}
	fmt.Println()
	// Output: a a b a c a a
}
