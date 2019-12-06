package fdbx

type options struct {
	lt     []byte
	gte    []byte
	limit  int
	idlen  int
	pflen  int
	filter Predicat
}

// Limit - max count of selected models
func Limit(n int) Option {
	return func(o *options) error {
		o.limit = n
		return nil
	}
}

// IDLen -
func IDLen(n int) Option {
	return func(o *options) error {
		o.idlen = n
		return nil
	}
}

// PrefixLen -
func PrefixLen(n int) Option {
	return func(o *options) error {
		o.pflen = n
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
