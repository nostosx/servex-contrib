package cache_test

import (
	"context"
	"fmt"
	"time"

	cacheloader "github.com/nostosx/servex-contrib/cache/loader"
	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"

	"github.com/nostosx/servex-contrib/cache/memory"
)

func Example() {
	owner, stop := context.WithCancel(context.Background())
	defer stop()
	loader, err := cacheloader.New[string](owner, memory.New(), 5*time.Second, cacheloader.WithTTL(time.Minute), cacheloader.WithCodec(jsoncodec.Codec))
	if err != nil {
		panic(err)
	}
	defer loader.Close()

	value, err := loader.Get(owner, "tenant-a:user:42", func(ctx context.Context) (string, error) {
		if err := context.Cause(ctx); err != nil {
			return "", err
		}
		return "alice", nil
	})
	fmt.Println(value, err)
	// Output: alice <nil>
}
