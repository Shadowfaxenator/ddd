package typeregistry

type option func(*registry)

func WithLogger(logger logger) option {
	return func(r *registry) {
		r.logger = logger
	}
}
