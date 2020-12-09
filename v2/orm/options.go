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

	if len(o.indexes) > 0 {
		o.batchidx = append(o.batchidx, o.simple2batch())
	}

	if len(o.multidx) > 0 {
		o.batchidx = append(o.batchidx, o.multi2batch())
	}

	return
}

type options struct {
	prefix   []byte
	reverse  bool
	creator  string
	lastkey  fdbx.Key
	vpack    uint64
	vwait    time.Duration
	delay    time.Duration
	refresh  time.Duration
	task     *models.TaskT
	headers  map[string]string
	indexes  map[uint16]IndexKey
	multidx  map[uint16]IndexMultiKey
	batchidx []IndexBatchKey
}

func Index(id uint16, f IndexKey) Option {
	return func(o *options) {
		if o.indexes == nil {
			o.indexes = make(map[uint16]IndexKey, 8)
		}
		if f != nil {
			o.indexes[id] = f
		}
	}
}

func MultiIndex(id uint16, f IndexMultiKey) Option {
	return func(o *options) {
		if o.multidx == nil {
			o.multidx = make(map[uint16]IndexMultiKey, 8)
		}
		if f != nil {
			o.multidx[id] = f
		}
	}
}

func BatchIndex(f IndexBatchKey) Option {
	return func(o *options) {
		if f == nil {
			o.batchidx = append(o.batchidx, f)
		}
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

func (o options) simple2batch() IndexBatchKey {
	return func(buf []byte) (res map[uint16][]fdbx.Key, err error) {
		var tmp fdbx.Key
		res = make(map[uint16][]fdbx.Key, len(o.indexes))

		for idx, fnc := range o.indexes {
			if tmp, err = fnc(buf); err != nil {
				return
			}

			if tmp == nil || len(tmp.Bytes()) == 0 {
				continue
			}

			res[idx] = append(res[idx], tmp)
		}

		return res, nil
	}
}

func (o options) multi2batch() IndexBatchKey {
	return func(buf []byte) (res map[uint16][]fdbx.Key, err error) {
		var keys []fdbx.Key
		res = make(map[uint16][]fdbx.Key, len(o.indexes))

		for idx, fnc := range o.multidx {
			if keys, err = fnc(buf); err != nil {
				return
			}

			if len(keys) == 0 {
				continue
			}

			for i := range keys {
				if keys[i] == nil || len(keys[i].Bytes()) == 0 {
					continue
				}

				res[idx] = append(res[idx], keys[i])
			}
		}

		return res, nil
	}
}
