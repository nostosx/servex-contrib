package validate_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/nostosx/servex-contrib/middleware/validate"
	"github.com/nostosx/servex/errorx"
)

// CreateUser is a request that validates itself.
type CreateUser struct{ Name string }

// Validate reports whether the request is well formed.
func (r *CreateUser) Validate() error {
	if r.Name == "" {
		return errors.New("name is required")
	}
	return nil
}

func Example() {
	ep := validate.New()(func(_ context.Context, req any) (any, error) {
		return "created " + req.(*CreateUser).Name, nil
	})

	resp, _ := ep(context.Background(), &CreateUser{Name: "alice"})
	fmt.Println(resp)

	_, err := ep(context.Background(), &CreateUser{})
	e := errorx.From(err)
	fmt.Println(e.Code, e.Message)
	// Output:
	// created alice
	// InvalidArgument name is required
}
