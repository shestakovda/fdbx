package orm

import "time"

func newOptions() options {
	return options{
		punch:   time.Second,
		indexes: make(map[uint16]IndexKey, 8),
	}
}

type options struct {
	punch   time.Duration
	indexes map[uint16]IndexKey
}

func Index(id uint16, f IndexKey) Option {
	return func(o *options) {
		o.indexes[id] = f
	}
}

func PunchTime(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.punch = d
		}
	}
}
