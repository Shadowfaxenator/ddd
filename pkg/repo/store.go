package repo

import "time"

type Msg struct {
	ID   string
	Body []byte
	Kind string
}

type StoredMsg struct {
	Msg
	Sequence  uint64
	Timestamp time.Time
}

type Snapshot struct {
	Body      []byte
	Timestamp time.Time
}
