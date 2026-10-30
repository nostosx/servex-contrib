package do

import (
	"fmt"
	"reflect"

	"github.com/nostosx/servex/di"
)

// ProvideConstructors registers ordinary constructors as lazy singletons. Each
// function must return T or (T, error), with fixed, exactly typed dependencies.
// Interface bindings use the declared return type; assignability is not inferred.
//
// All signatures and duplicate outputs in this batch are checked before any
// registration. A Container.Register failure can leave preceding registrations
// installed, but no constructor is invoked during registration. Reflection runs
// only while constructing the dependency graph; resolve application roots once
// during assembly rather than resolving from request handlers.
//
// Resource ownership is unchanged: this helper discovers no Close/Stop methods
// and registers no implicit cleanup. Constructors remain ordinary Go functions.
func ProvideConstructors(registry di.Container, constructors ...any) error {
	if registry == nil || nilReflectValue(reflect.ValueOf(registry)) {
		return fmt.Errorf("%w: nil container", di.ErrInvalidArgument)
	}
	providers := make([]constructor, 0, len(constructors))
	outputs := make(map[reflect.Type]bool, len(constructors))
	for index, function := range constructors {
		provider, err := inspectConstructor(function)
		if err != nil {
			return fmt.Errorf("di: constructor %d: %w", index, err)
		}
		if outputs[provider.output] {
			return fmt.Errorf("%w: constructor output %v", di.ErrDuplicate, provider.output)
		}
		outputs[provider.output] = true
		providers = append(providers, provider)
	}
	for _, provider := range providers {
		if err := registry.Register(provider.output, provider.provide); err != nil {
			return fmt.Errorf("di: register constructor for %v: %w", provider.output, err)
		}
	}
	return nil
}

type constructor struct {
	function reflect.Value
	inputs   []reflect.Type
	output   reflect.Type
	hasError bool
}

func inspectConstructor(function any) (constructor, error) {
	value := reflect.ValueOf(function)
	if !value.IsValid() || value.Kind() != reflect.Func {
		return constructor{}, fmt.Errorf("%w: constructor must be a function", di.ErrInvalidArgument)
	}
	if value.IsNil() {
		return constructor{}, fmt.Errorf("%w: nil constructor", di.ErrInvalidArgument)
	}
	typ := value.Type()
	if typ.IsVariadic() {
		return constructor{}, fmt.Errorf("%w: variadic constructors require an explicit wrapper", di.ErrInvalidArgument)
	}
	if typ.NumOut() < 1 || typ.NumOut() > 2 {
		return constructor{}, fmt.Errorf("%w: constructor must return T or (T, error)", di.ErrInvalidArgument)
	}
	if typ.Out(0) == reflect.TypeFor[error]() {
		return constructor{}, fmt.Errorf("%w: constructor must return a service, not only an error", di.ErrInvalidArgument)
	}
	if typ.NumOut() == 2 && typ.Out(1) != reflect.TypeFor[error]() {
		return constructor{}, fmt.Errorf("%w: constructor's second result must be error", di.ErrInvalidArgument)
	}
	inputs := make([]reflect.Type, typ.NumIn())
	for index := range inputs {
		inputs[index] = typ.In(index)
	}
	return constructor{
		function: value,
		inputs:   inputs,
		output:   typ.Out(0),
		hasError: typ.NumOut() == 2,
	}, nil
}

func (c constructor) provide(resolver di.Resolver) (any, error) {
	arguments := make([]reflect.Value, len(c.inputs))
	for index, input := range c.inputs {
		value, err := resolver.Resolve(input)
		if err != nil {
			return nil, fmt.Errorf("di: constructor %v dependency %v: %w", c.output, input, err)
		}
		if value == nil {
			zero := reflect.Zero(input)
			if !nilReflectValue(zero) {
				return nil, fmt.Errorf("%w: nil dependency for %v", di.ErrInvalidArgument, input)
			}
			arguments[index] = zero
			continue
		}
		argument := reflect.ValueOf(value)
		if !argument.Type().AssignableTo(input) {
			return nil, fmt.Errorf("%w: dependency %T cannot supply %v", di.ErrInvalidArgument, value, input)
		}
		arguments[index] = argument
	}
	results := c.function.Call(arguments)
	if c.hasError && !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}
	return results[0].Interface(), nil
}

func nilReflectValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return value.IsNil()
	default:
		return false
	}
}
