package orm

func newOptions() options {
	return options{
		indexes: make(map[byte]IndexKey, 8),
	}
}

type options struct {
	indexes map[byte]IndexKey
}

func Index(id byte, f IndexKey) Option { return func(o *options) { o.indexes[id] = f } }
