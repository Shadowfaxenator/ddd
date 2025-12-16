package aggregate

import "github.com/google/uuid"

type Aggregatable any

type ID[T any] string

func (i ID[T]) String() string {
	return string(i)
}

func (i ID[T]) UUID() uuid.UUID {
	u, err := uuid.Parse(string(i))
	if err != nil {
		panic(err)
	}
	return u
}
