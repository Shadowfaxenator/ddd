package aggregate

import "errors"

var (
	ErrNotExists     = errors.New("no aggregate messages")
	ErrAlreadyExists = errors.New("aggregate already exists")
)
