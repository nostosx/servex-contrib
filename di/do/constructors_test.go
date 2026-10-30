package do_test

import (
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	dox "github.com/nostosx/servex-contrib/di/do"
	"github.com/nostosx/servex/di"
)

type constructorConfig struct{ message string }
type constructorRepository struct{ message string }
type constructorService struct{ repository *constructorRepository }

func TestProvideConstructorsPlainGraph(t *testing.T) {
	t.Parallel()
	c := newContainer(t)
	mustRegister(t, di.Supply(c, constructorConfig{message: "hello"}))
	var calls atomic.Int32
	mustRegister(t, dox.ProvideConstructors(c,
		func(config constructorConfig) *constructorRepository {
			calls.Add(1)
			return &constructorRepository{message: config.message}
		},
		func(repository *constructorRepository) (*constructorService, error) {
			return &constructorService{repository: repository}, nil
		},
	))
	if calls.Load() != 0 {
		t.Fatal("constructor ran during registration")
	}
	var wg sync.WaitGroup
	services := make([]*constructorService, 12)
	for index := range services {
		wg.Go(func() {
			service, err := di.Resolve[*constructorService](c)
			if err != nil {
				t.Error(err)
			}
			services[index] = service
		})
	}
	wg.Wait()
	for _, service := range services {
		if service == nil || service != services[0] || service.repository.message != "hello" {
			t.Fatalf("unexpected service: %+v", service)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("constructor calls = %d", calls.Load())
	}
}

func TestProvideConstructorsErrors(t *testing.T) {
	t.Parallel()
	t.Run("constructor failure", func(t *testing.T) {
		c := newContainer(t)
		want := errors.New("database unavailable")
		mustRegister(t, dox.ProvideConstructors(c, func() (*constructorRepository, error) { return nil, want }))
		if _, err := di.Resolve[*constructorRepository](c); !errors.Is(err, want) {
			t.Fatalf("lost constructor error: %v", err)
		}
	})
	t.Run("missing dependency", func(t *testing.T) {
		c := newContainer(t)
		mustRegister(t, dox.ProvideConstructors(c, func(config constructorConfig) *constructorRepository {
			return &constructorRepository{message: config.message}
		}))
		if _, err := di.Resolve[*constructorRepository](c); !errors.Is(err, di.ErrNotFound) {
			t.Fatalf("lost missing dependency: %v", err)
		}
	})
	t.Run("nil dependency", func(t *testing.T) {
		c := newContainer(t)
		mustRegister(t, di.Supply[io.Reader](c, nil))
		mustRegister(t, dox.ProvideConstructors(c, func(reader io.Reader) string {
			if reader == nil {
				return "no reader"
			}
			return "reader"
		}))
		value, err := di.Resolve[string](c)
		if err != nil || value != "no reader" {
			t.Fatal(value, err)
		}
	})
}

func TestProvideConstructorsValidateWholeBatch(t *testing.T) {
	t.Parallel()
	var nilFunction func() string
	for _, test := range []struct {
		name        string
		constructor any
	}{
		{name: "nil", constructor: nil},
		{name: "typed nil", constructor: nilFunction},
		{name: "non-function", constructor: 42},
		{name: "no result", constructor: func() {}},
		{name: "error only", constructor: func() error { return nil }},
		{name: "wrong second result", constructor: func() (string, bool) { return "", true }},
		{name: "too many results", constructor: func() (string, int, error) { return "", 0, nil }},
		{name: "variadic", constructor: func(...int) string { return "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newContainer(t)
			if err := dox.ProvideConstructors(c, func() int { return 1 }, test.constructor); !errors.Is(err, di.ErrInvalidArgument) {
				t.Fatalf("invalid constructor accepted: %v", err)
			}
			if _, err := di.Resolve[int](c); !errors.Is(err, di.ErrNotFound) {
				t.Fatalf("partially registered invalid batch: %v", err)
			}
		})
	}
	c := newContainer(t)
	if err := dox.ProvideConstructors(c, func() int { return 1 }, func() int { return 2 }); !errors.Is(err, di.ErrDuplicate) {
		t.Fatalf("duplicate output accepted: %v", err)
	}
	if _, err := di.Resolve[int](c); !errors.Is(err, di.ErrNotFound) {
		t.Fatalf("duplicate batch partly registered: %v", err)
	}
	var nilContainer *dox.Container
	if err := dox.ProvideConstructors(nilContainer, func() int { return 1 }); !errors.Is(err, di.ErrInvalidArgument) {
		t.Fatalf("nil container accepted: %v", err)
	}
}

func TestProvideConstructorsExactInterfaceBinding(t *testing.T) {
	t.Parallel()
	c := newContainer(t)
	mustRegister(t, dox.ProvideConstructors(c,
		func() *strings.Reader { return strings.NewReader("concrete") },
		func(reader io.Reader) (string, error) {
			value, err := io.ReadAll(reader)
			return string(value), err
		},
	))
	if _, err := di.Resolve[string](c); !errors.Is(err, di.ErrNotFound) {
		t.Fatalf("interface binding was inferred: %v", err)
	}
	mustRegister(t, dox.ProvideConstructors(c, func() io.Reader { return strings.NewReader("interface") }))
	value, err := di.Resolve[string](c)
	if err != nil || value != "interface" {
		t.Fatal(value, err)
	}
}
