package identity

import (
	"strconv"
	"time"

	"github.com/sony/sonyflake/v2"
)

var sf *sonyflake.Sonyflake

func init() {

	var err error
	sf, err = sonyflake.New(sonyflake.Settings{
		BitsSequence: 12,
		TimeUnit:     2 * time.Millisecond,
	})
	if err != nil {
		panic(err)
	}
}

func New() (ID, error) {

	id, err := sf.NextID()
	if err != nil {
		return 0, err
	}
	return ID(id), nil
}
func (i ID) Int64() int64 {
	return int64(i)
}
func MustParseIDFromString(s string) ID {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return ID(id)
}

type ID int64


func (i ID) String() string {

	return strconv.FormatInt(int64(i), 10)
}
