package typeregistry

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"sync"

	"github.com/alekseev-bro/ddd/internal/prettylog"
)

type registry struct {
	mu     sync.RWMutex
	ctors  map[string]ctor
	types  map[reflect.Type]string
	logger logger
}

type logger interface {
	Warn(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
}

func New(opts ...option) *registry {
	r := &registry{
		logger: prettylog.NewDefault(),
		ctors:  make(map[string]ctor),
		types:  make(map[reflect.Type]string),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

type ctor = func() any

// Register registers a type, it's not thread safe
func (r *registry) Register(tname string, c ctor) error {

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.types[reflect.TypeOf(c())]; ok {
		return fmt.Errorf("type %T already registered", c())
	}
	if _, ok := r.ctors[tname]; ok {
		return fmt.Errorf("type %s already registered", tname)
	}
	r.types[reflect.TypeOf(c())] = tname
	r.ctors[tname] = c
	r.logger.Info("type registered", "kind", tname)
	return nil
}

func (r *registry) Create(name string) (any, error) {
	r.mu.RLock()
	ct, ok := r.ctors[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("registry: unknown type name %q", name)
	}
	return ct(), nil
}

func (r *registry) Kind(in any) (string, error) {
	if in == nil {
		return "", errors.New("registry: cannot get name for nil")
	}

	t := reflect.TypeOf(in)

	r.mu.RLock()
	name, ok := r.types[t]
	r.mu.RUnlock()

	if !ok {
		if t.Kind() != reflect.Pointer {
			pt := reflect.PointerTo(t)
			if _, ptrOK := r.types[pt]; ptrOK {
				return "", fmt.Errorf("registry: %v is registered as %v; emit pointer event", t, pt)
			}
		}
		return "", fmt.Errorf("registry: type %v is not registered", t)
	}
	return name, nil
}

func TypeNameFor[T any](opts ...typeNameFromOption) string {
	var zero T
	return TypeNameFrom(zero, opts...)
}

type typeNameFromOption string

func WithDelimiter(delimiter string) typeNameFromOption {
	return typeNameFromOption(delimiter)
}

type Kinder interface {
	Kind(in any) (string, error)
}

type Creator interface {
	Create(name string) (any, error)
}

type CreateKinderRegistry interface {
	Register(tname string, c func() any) error
	Creator
	Kinder
}

func TypeNameFrom(e any, opts ...typeNameFromOption) string {
	delim := "::"
	for _, opt := range opts {
		delim = string(opt)
	}
	t := reflect.TypeOf(e)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	sha := sha1.New()
	sha.Write([]byte(t.PkgPath()))
	bctx := base64.RawURLEncoding.EncodeToString(sha.Sum(nil)[:5])

	switch t.Kind() {

	case reflect.Struct:
		return fmt.Sprintf("%s%s%s", t.Name(), delim, bctx)
	case reflect.Pointer:
		return fmt.Sprintf("%s%s%s", t.Elem().Name(), delim, bctx)
	default:
		return fmt.Sprintf("%s%s%s", strconv.FormatUint(rand.Uint64(), 10), delim, bctx)
	}

}
