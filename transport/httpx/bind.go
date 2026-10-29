package httpx

import (
	stdencoding "encoding"
	"fmt"
	"maps"
	"mime/multipart"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// Each route owns a bounded cache. Dynamic request factories may return many
// types; exceeding the bound falls back to building a plan without retaining it.
const maxBindingPlans = 128

type bindingCache struct {
	mu    sync.RWMutex
	plans map[reflect.Type]*bindingPlan
}

type bindingField struct {
	name  string
	index []int
}

type bindingPlan struct {
	values []bindingField
	files  map[string][]int
}

func newBindingPlan(t reflect.Type) *bindingPlan {
	fields := fieldIndexes(t, isValueType)
	plan := &bindingPlan{files: fieldIndexes(t, isFileType)}
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		plan.values = append(plan.values, bindingField{name: name, index: fields[name]})
	}
	return plan
}

func (c *bindingCache) plan(t reflect.Type) *bindingPlan {
	if c == nil {
		return newBindingPlan(t)
	}
	c.mu.RLock()
	plan := c.plans[t]
	c.mu.RUnlock()
	if plan != nil {
		return plan
	}
	plan = newBindingPlan(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.plans[t]; existing != nil {
		return existing
	}
	if len(c.plans) < maxBindingPlans {
		if c.plans == nil {
			c.plans = make(map[reflect.Type]*bindingPlan)
		}
		c.plans[t] = plan
	}
	return plan
}

// Reflected types used by the binder.
var (
	textUnmarshalerType = reflect.TypeFor[stdencoding.TextUnmarshaler]()
	fileHeaderType      = reflect.TypeFor[*multipart.FileHeader]()
	fileHeadersType     = reflect.TypeFor[[]*multipart.FileHeader]()
)

// bindValues assigns values to the fields of the struct dst points to,
// matching keys against JSON field names. Scalars take the first value,
// slices take all of them. Fields of unsupported types are left alone, and
// so is dst when it is not a pointer to a struct.
func bindValues(dst any, values map[string][]string) error {
	return (*bindingCache)(nil).bindValues(dst, values)
}

func (c *bindingCache) bindValues(dst any, values map[string][]string) error {
	if len(values) == 0 {
		return nil
	}
	root, ok := structPointer(dst)
	if !ok {
		return nil
	}
	for _, field := range c.plan(root.Type()).values {
		vals := values[field.name]
		if len(vals) == 0 {
			continue
		}
		f, err := fieldByIndex(root, field.index)
		if err != nil {
			return err
		}
		if err := setValues(f, vals); err != nil {
			return fmt.Errorf("field %q: %w", field.name, err)
		}
	}
	return nil
}

// bindFiles assigns uploaded files to *multipart.FileHeader and
// []*multipart.FileHeader fields matched by JSON field name.
func bindFiles(dst any, files map[string][]*multipart.FileHeader) error {
	return (*bindingCache)(nil).bindFiles(dst, files)
}

func (c *bindingCache) bindFiles(dst any, files map[string][]*multipart.FileHeader) error {
	if len(files) == 0 {
		return nil
	}
	root, ok := structPointer(dst)
	if !ok {
		return nil
	}
	for name, index := range c.plan(root.Type()).files {
		fhs := files[name]
		if len(fhs) == 0 {
			continue
		}
		f, err := fieldByIndex(root, index)
		if err != nil {
			return err
		}
		if f.Type() == fileHeaderType {
			f.Set(reflect.ValueOf(fhs[0]))
		} else {
			f.Set(reflect.ValueOf(slices.Clone(fhs)))
		}
	}
	return nil
}

// structPointer returns the struct dst points to.
func structPointer(dst any) (reflect.Value, bool) {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	return v.Elem(), true
}

// fieldIndexes maps the JSON names of the fields of struct type t whose
// type satisfies accept to their index paths. Fields of embedded structs
// without a JSON name are promoted; shallower fields win over deeper ones.
func fieldIndexes(t reflect.Type, accept func(reflect.Type) bool) map[string][]int {
	out := make(map[string][]int)
	collectFields(t, nil, nil, accept, out)
	return out
}

// collectFields adds the fields of t, prefixed by the index path prefix, to
// out. Direct fields are added before the fields of embedded structs. The type
// path prevents recursive embedding from revisiting an ancestor; sibling
// branches can still promote fields from the same type.
func collectFields(t reflect.Type, prefix []int, path []reflect.Type, accept func(reflect.Type) bool, out map[string][]int) {
	if slices.Contains(path, t) {
		return
	}
	path = append(path, t)
	var embedded []reflect.StructField
	for f := range t.Fields() {
		name, tagged, ok := jsonName(f)
		if !ok {
			continue
		}
		if f.Anonymous && !tagged {
			ft := f.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct && !ft.Implements(textUnmarshalerType) && !reflect.PointerTo(ft).Implements(textUnmarshalerType) {
				embedded = append(embedded, f)
				continue
			}
		}
		if !f.IsExported() || !accept(f.Type) {
			continue
		}
		if _, dup := out[name]; !dup {
			out[name] = append(slices.Clone(prefix), f.Index...)
		}
	}
	for _, f := range embedded {
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		collectFields(ft, append(slices.Clone(prefix), f.Index...), path, accept, out)
	}
}

// jsonName returns the JSON name of f, whether the json tag names it
// explicitly, and false when f is excluded with `json:"-"`.
func jsonName(f reflect.StructField) (name string, tagged, ok bool) {
	tag := f.Tag.Get("json")
	if tag == "-" {
		return "", false, false
	}
	name, _, _ = strings.Cut(tag, ",")
	if name == "" {
		return f.Name, false, true
	}
	return name, true, true
}

// fieldByIndex returns the field at index, allocating nil embedded pointers
// on the way.
func fieldByIndex(v reflect.Value, index []int) (reflect.Value, error) {
	for i, x := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				if !v.CanSet() {
					return reflect.Value{}, fmt.Errorf("cannot allocate embedded pointer to unexported %v", v.Type().Elem())
				}
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(x)
	}
	return v, nil
}

// isFileType reports whether t can receive uploaded files.
func isFileType(t reflect.Type) bool {
	return t == fileHeaderType || t == fileHeadersType
}

// isValueType reports whether t can be bound from string values: a scalar,
// a pointer to a scalar, or a slice of scalars.
func isValueType(t reflect.Type) bool {
	if isScalarType(t) {
		return true
	}
	switch t.Kind() {
	case reflect.Pointer:
		return isScalarType(t.Elem())
	case reflect.Slice:
		return t.Elem().Kind() != reflect.Uint8 && isScalarType(t.Elem())
	}
	return false
}

// isScalarType reports whether a single string can be parsed into t.
func isScalarType(t reflect.Type) bool {
	if reflect.PointerTo(t).Implements(textUnmarshalerType) {
		return true
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// setValues stores vals into the settable field v, whose type satisfies
// isValueType.
func setValues(v reflect.Value, vals []string) error {
	t := v.Type()
	if isScalarType(t) {
		return setScalar(v, vals[0])
	}
	switch t.Kind() {
	case reflect.Pointer:
		p := reflect.New(t.Elem())
		if err := setScalar(p.Elem(), vals[0]); err != nil {
			return err
		}
		v.Set(p)
	case reflect.Slice:
		s := reflect.MakeSlice(t, len(vals), len(vals))
		for i, val := range vals {
			if err := setScalar(s.Index(i), val); err != nil {
				return err
			}
		}
		v.Set(s)
	}
	return nil
}

// setScalar parses s into the settable value v, whose type satisfies
// isScalarType.
func setScalar(v reflect.Value, s string) error {
	if u, ok := reflect.TypeAssert[stdencoding.TextUnmarshaler](v.Addr()); ok {
		return u.UnmarshalText([]byte(s))
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(s)
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(s, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetFloat(f)
	}
	return nil
}

// pathNames returns the wildcard names of a ServeMux pattern, without the
// "..." suffix and without the "{$}" anchor.
func pathNames(pattern string) []string {
	var names []string
	for seg := range strings.SplitSeq(pattern, "/") {
		inner, ok := strings.CutPrefix(seg, "{")
		if !ok {
			continue
		}
		inner, ok = strings.CutSuffix(inner, "}")
		if !ok {
			continue
		}
		inner, _ = strings.CutSuffix(inner, "...")
		if inner != "" && inner != "$" {
			names = append(names, inner)
		}
	}
	return names
}
