package aggregate

import (
	"github.com/alekseev-bro/ddd/pkg/identity"
)

type ID identity.ID

func (i ID) I64() int64 {
	return int64(i)
}

func NewID() (ID, error) {
	i, err := identity.New()
	if err != nil {
		return ID(0), err
	}
	return ID(i), nil
}

func (i ID) String() string {
	return identity.ID(i).String()
}
