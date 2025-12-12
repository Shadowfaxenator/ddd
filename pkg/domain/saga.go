package domain

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/alekseev-bro/ddd/internal/typereg"
)

type sagaHandlerFunc[E Event[T], C Command[U], T any, U any] func(event E) C

type sagaHandler[E Event[T], C Command[U], T any, U any] struct {
	sub     projector[T]
	cmd     executer[U]
	handler sagaHandlerFunc[E, C, T, U]
	ot      OrderingType
}

func (sf *sagaHandler[E, C, T, U]) Handle(ctx context.Context, eventID EventID[T], event Event[T]) error {

	return sf.cmd.Execute(ctx, eventID.String(), sf.handler(event.(E)))

}

type sagaOption[E Event[T], C Command[U], T any, U any] func(*sagaHandler[E, C, T, U])

func WithOrdering[E Event[T], C Command[U], T any, U any](ot OrderingType) sagaOption[E, C, T, U] {
	return func(sh *sagaHandler[E, C, T, U]) {
		sh.ot = ot
	}
}

func Saga[E Event[T], C Command[U], T any, U any](ctx context.Context, sub projector[T], cmd executer[U], shf sagaHandlerFunc[E, C, T, U], opts ...sagaOption[E, C, T, U]) Drainer {

	sh := &sagaHandler[E, C, T, U]{
		ot:      Ordered,
		sub:     sub,
		cmd:     cmd,
		handler: shf,
	}

	for _, opt := range opts {
		opt(sh)
	}
	var (
		ee E
		cc C
		uu U
		tt T
	)

	ename := typereg.TypeNameFrom(ee)
	cname := typereg.TypeNameFrom(cc)
	sname := typereg.TypeNameFrom(tt)
	cmname := typereg.TypeNameFrom(uu)
	durname := fmt.Sprintf("%s:%s|%s:%s", sname, ename, cmname, cname)

	order := func() ProjOption {
		if sh.ot == Ordered {
			return nil
		}
		return WithUnordered()
	}
	d, err := sub.Project(ctx, sh, WithName(durname), order(), WithFilterByEvent[E]())
	if err != nil {
		slog.Error("failed to project saga handler", "error", err)
		panic(err)
	}

	return d

}
