package serde

import "encoding/json"

type Serder interface {
	Serialize(v any) ([]byte, error)
	Deserialize(b []byte, out any) error
}

func Serialize(v any) ([]byte, error) {
	return defaultSerder.Serialize(v)
}

func Deserialize(b []byte, out any) error {
	return defaultSerder.Deserialize(b, out)
}

func SetDefaultSerder(s Serder) {
	defaultSerder = s
}

// default is json
var defaultSerder Serder = &jsonSerder{}

type jsonSerder struct{}

func (*jsonSerder) Serialize(v any) ([]byte, error) {

	return json.Marshal(v)
}

func (*jsonSerder) Deserialize(b []byte, out any) error {

	return json.Unmarshal(b, out)
}
