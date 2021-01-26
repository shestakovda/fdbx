package orm

import (
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/shestakovda/fdbx/v2/models"
)

func getOpts(args []Option) (o options) {
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
	lastkey  fdb.Key
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

//goland:noinspection GoUnusedExportedFunction
func BatchIndex(f IndexBatchKey) Option {
	return func(o *options) {
		if f != nil {
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

//goland:noinspection GoUnusedExportedFunction
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

func LastKey(k fdb.Key) Option {
	return func(o *options) {
		o.lastkey = k
	}
}

func Delay(d time.Duration) Option {
	return func(o *options) {
		o.delay = d
	}
}

//goland:noinspection GoUnusedExportedFunction
func Creator(s string) Option {
	return func(o *options) {
		o.creator = s
	}
}

//goland:noinspection GoUnusedExportedFunction
func Header(k, v string) Option {
	return func(o *options) {
		if o.headers == nil {
			o.headers = make(map[string]string, 1)
		}
		o.headers[k] = v
	}
}

//goland:noinspection GoUnusedExportedFunction
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
	return func(buf []byte) (res map[uint16][]fdb.Key, err error) {
		var tmp fdb.Key
		res = make(map[uint16][]fdb.Key, len(o.indexes))

		for idx, fnc := range o.indexes {
			if tmp, err = fnc(buf); err != nil {
				return
			}

			if len(tmp) == 0 {
				continue
			}

			res[idx] = append(res[idx], tmp)
		}

		return res, nil
	}
}

func (o options) multi2batch() IndexBatchKey {
	return func(buf []byte) (res map[uint16][]fdb.Key, err error) {
		var keys []fdb.Key
		res = make(map[uint16][]fdb.Key, len(o.indexes))

		for idx, fnc := range o.multidx {
			if keys, err = fnc(buf); err != nil {
				return
			}

			if len(keys) == 0 {
				continue
			}

			for i := range keys {
				if len(keys[i]) == 0 {
					continue
				}

				res[idx] = append(res[idx], keys[i])
			}
		}

		return res, nil
	}
}
