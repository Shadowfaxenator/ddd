package typeregistry

type option func(*registry)

func WithLogger(logger logger) option {
	return func(r *registry) {
		r.logger = logger
	}
}

type typeNameFromOptions struct {
	delimiter string
	noPkg     bool
}

type typeNameFromOption func(*typeNameFromOptions)

func WithDelimiter(delimiter string) typeNameFromOption {
	return func(opts *typeNameFromOptions) {
		opts.delimiter = delimiter
	}
}

func WithNoPkg() typeNameFromOption {
	return func(opts *typeNameFromOptions) {
		opts.noPkg = true
	}
}

func WithTypeNameFromOptions(opts ...typeNameFromOption) typeNameFromOption {
	return func(options *typeNameFromOptions) {
		for _, opt := range opts {
			opt(options)
		}
	}
}
