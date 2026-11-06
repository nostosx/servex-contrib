package fx_test

import (
	"context"
	"fmt"

	uberfx "go.uber.org/fx"

	fxbridge "github.com/nostosx/servex-contrib/di/fx"
	"github.com/nostosx/servex/app"
)

type greeter struct{ message string }

func newGreeter(message string) *greeter { return &greeter{message: message} }

func ExampleBridge_Run() {
	var service *greeter
	var shutdowner uberfx.Shutdowner
	native := uberfx.New(
		uberfx.NopLogger,
		uberfx.Supply("hello"),
		uberfx.Provide(newGreeter),
		uberfx.Populate(&service, &shutdowner),
	)
	dependencies := fxbridge.Hook("dependencies", native)
	worker := app.NewHook("worker", func(context.Context) error {
		fmt.Println(service.message)
		return shutdowner.Shutdown()
	}, nil)
	application := app.New(app.WithHooks(dependencies, worker))
	if err := dependencies.Run(context.Background(), application); err != nil {
		panic(err)
	}
	// Output: hello
}
