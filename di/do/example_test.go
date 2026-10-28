package do_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/samber/do/v2"

	dox "github.com/nostosx/servex-contrib/di/do"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/di"
)

type greeter struct{ message string }

func newGreeter(message string) *greeter { return &greeter{message: message} }

func ExampleNew() {
	container := dox.New()
	defer func() {
		if err := container.Stop(context.Background()); err != nil {
			panic(err)
		}
	}()
	var registry di.Container = container
	if err := di.Supply(registry, "hello"); err != nil {
		panic(err)
	}
	if err := di.Provide(registry, func(r di.Resolver) (*greeter, error) {
		return newGreeter(di.MustResolve[string](r)), nil
	}); err != nil {
		panic(err)
	}
	service, err := di.Resolve[*greeter](registry)
	if err != nil {
		panic(err)
	}
	fmt.Println(service.message)
	// Output: hello
}

func ExampleHook() {
	injector := do.New()
	dependencies := dox.Hook("dependencies", injector)
	// This fallback also covers failed assembly and an unstarted App.
	defer func() {
		if err := dependencies.Stop(context.Background()); err != nil {
			panic(err)
		}
	}()

	do.ProvideValue(injector, "hello")
	do.Provide(injector, func(i do.Injector) (*greeter, error) {
		return newGreeter(do.MustInvoke[string](i)), nil
	})
	service, err := do.Invoke[*greeter](injector)
	if err != nil {
		panic(err)
	}

	worker := app.NewHook("worker", func(context.Context) error {
		fmt.Println(service.message)
		return nil
	}, nil)
	application := app.New(
		app.WithHooks(dependencies, worker),
		app.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err := application.Start(context.Background()); err != nil {
		panic(err)
	}
	if err := application.Stop(context.Background()); err != nil {
		panic(err)
	}
	// Output: hello
}
