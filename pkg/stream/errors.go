package stream

type NonRetriableError struct {
	Err error
}

func (e NonRetriableError) Error() string {
	return e.Err.Error()
}
