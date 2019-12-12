package fdbx

type options struct {
	to     []byte
	from   []byte
	limit  uint
	filter Predicat
}

// Limit - max count of selected models
func Limit(n uint) Option {
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

// From - greater then or equal
func From(value []byte) Option {
	return func(o *options) error {
		o.from = value
		return nil
	}
}

// To - less then
func To(value []byte) Option {
	return func(o *options) error {
		o.to = value
		return nil
	}
}

// Query - rows by prefix
func Query(value []byte) Option {
	return func(o *options) error {
		o.from = value
		o.to = append(value, 0xFF)
		return nil
	}
}
