package orm

import "time"

func newOptions() options {
	return options{
		punch:   time.Second,
		indexes: make(map[byte]IndexKey, 8),
	}
}

type options struct {
	punch   time.Duration
	indexes map[byte]IndexKey
}

func Index(id byte, f IndexKey) Option {
	return func(o *options) {
		if id != nsData && id != nsBLOB {
			o.indexes[id] = f
		}
	}
}

func PunchTime(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.punch = d
		}
	}
}
