package orm

import (
	"time"

	"github.com/shestakovda/fdbx/v2"
)

func getOpts(args []Option) (o options) {
	o.vpack = 1000
	o.vwait = time.Hour
	o.refresh = time.Second

	for i := range args {
		args[i](&o)
	}
	return
}

type options struct {
	prefix  []byte
	reverse bool
	lastkey fdbx.Key
	vpack   uint64
	vwait   time.Duration
	refresh time.Duration
	indexes map[uint16]IndexKey
}

func Index(id uint16, f IndexKey) Option {
	return func(o *options) {
		if o.indexes == nil {
			o.indexes = make(map[uint16]IndexKey, 8)
		}
		o.indexes[id] = f
	}
}

func Refresh(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.refresh = d
		}
	}
}

func VacuumPack(d int) Option {
	return func(o *options) {
		if d > 0 {
			o.vpack = uint64(d)
		}
	}
}

func VacuumWait(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.vwait = d
		}
	}
}

func Prefix(p []byte) Option {
	return func(o *options) {
		o.prefix = p
	}
}

func Reverse(r bool) Option {
	return func(o *options) {
		o.reverse = r
	}
}

func LastKey(k fdbx.Key) Option {
	return func(o *options) {
		o.lastkey = k
	}
}
