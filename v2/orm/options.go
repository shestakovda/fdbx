package orm

import (
	"time"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
)

func getOpts(args []Option) (o options) {
	o.vpack = 1000
	o.vwait = time.Hour
	o.refresh = time.Minute

	for i := range args {
		args[i](&o)
	}
	return
}

type options struct {
	prefix  []byte
	reverse bool
	creator string
	lastkey fdbx.Key
	vpack   uint64
	vwait   time.Duration
	delay   time.Duration
	refresh time.Duration
	task    *models.TaskT
	headers map[string]string
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

func Delay(d time.Duration) Option {
	return func(o *options) {
		o.delay = d
	}
}

func Creator(s string) Option {
	return func(o *options) {
		o.creator = s
	}
}

func Header(k, v string) Option {
	return func(o *options) {
		if o.headers == nil {
			o.headers = make(map[string]string, 1)
		}
		o.headers[k] = v
	}
}

func Headers(h map[string]string) Option {
	return func(o *options) {
		o.headers = h
	}
}

func metatask(t *models.TaskT) Option {
	return func(o *options) {
		o.task = t
	}
}
