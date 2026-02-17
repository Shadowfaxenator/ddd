package typeregistry

type option func(*registry)

func WithLogger(logger logger) option {
	return func(r *registry) {
		r.logger = logger
	}
}

type createNameOptions struct {
	delimiter string
	noPkg     bool
	plainPkg  bool
}

type createNameOption func(*createNameOptions)

func WithDelimiter(delimiter string) createNameOption {
	return func(opts *createNameOptions) {
		opts.delimiter = delimiter
	}
}

func WithNoPkg() createNameOption {
	return func(opts *createNameOptions) {
		opts.noPkg = true
	}
}

func WithPlainTextPkg() createNameOption {
	return func(opts *createNameOptions) {
		opts.plainPkg = true
	}
}
