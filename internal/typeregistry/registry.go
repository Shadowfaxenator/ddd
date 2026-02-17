package typeregistry

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
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

func TypeNameFor[T any](opts ...createNameOption) string {
	var zero T
	return CreateNameFromType(zero, opts...)
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

func CreateNameFromType(e any, opts ...createNameOption) string {

	cfg := &createNameOptions{delimiter: "::"}
	for _, opt := range opts {
		opt(cfg)
	}
	t := reflect.TypeOf(e)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	var bctx string

	if cfg.plainPkg {
		s := strings.Split(t.PkgPath(), "/")
		bctx = s[len(s)-1]
	} else {
		sha := sha1.New()
		sha.Write([]byte(t.PkgPath()))
		//	bctx = hex.EncodeToString(sha.Sum(nil)[:5])
		bctx = base64.RawURLEncoding.EncodeToString(sha.Sum(nil)[:5])
	}

	var tname string
	switch t.Kind() {
	case reflect.Struct:
		tname = t.Name()
	case reflect.Pointer:
		tname = t.Elem().Name()
	default:
		tname = strconv.FormatUint(rand.Uint64(), 10)
	}
	if cfg.noPkg {
		return tname
	}
	return fmt.Sprintf("%s%s%s", bctx, cfg.delimiter, tname)
}
