package fdbx

type options struct {
	lt     []byte
	gte    []byte
	limit  int
	filter Predicat
}

// Limit - max count of selected models
func Limit(n int) Option {
	return func(o *options) error {
		o.limit = n
		return nil
	}
}

// Filter -
func Filter(f Predicat) Option {
	return func(o *options) error {
		o.filter = f
		return nil
	}
}

// GTE - greater then or equal
func GTE(q []byte) Option {
	return func(o *options) error {
		o.gte = q
		return nil
	}
}

// LT - less then
func LT(q []byte) Option {
	return func(o *options) error {
		o.lt = q
		return nil
	}
}
