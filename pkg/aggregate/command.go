package aggregate

// All commands must implement the Command interface.
type Command[T Aggregatable] interface {
	// Entity can't be nil
	Execute(entity *T) Event[T]
	identer[T]
}

type identer[T Aggregatable] interface {
	AggregateID() ID[T]
}

// Is an alias for ID[Command[T]]
type CommandID[T Aggregatable] = ID[Command[T]]
