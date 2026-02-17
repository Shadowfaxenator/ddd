package stream

import "errors"

var (
	ErrNoEvents = errors.New("no events")
)

type NonRetriableError struct {
	Err error
}

func (e NonRetriableError) Error() string {
	return e.Err.Error()
}
