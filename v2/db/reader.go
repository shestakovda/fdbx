package db

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// ReadHandler - обработчик физической транзакции чтения, должен быть идемпотентным
type ReadHandler func(Reader) error

type Reader struct {
	Connection
	tx fdb.ReadTransaction
}

// Получение объекта ожидания конкретного значения
func (r Reader) Data(key fdb.Key) []byte {
	return r.tx.Get(r.usrWrap(key)).MustGet()
}

func (r Reader) List(from, last fdb.Key, limit uint64, reverse, skip bool) fdb.RangeResult {
	var rng fdb.Range

	if !skip {
		rng = &fdb.KeyRange{
			Begin: r.usrWrap(from),
			End:   r.endWrap(last),
		}
	} else if reverse {
		rng = &fdb.SelectorRange{
			Begin: fdb.FirstGreaterOrEqual(r.usrWrap(from)),
			End: &fdb.KeySelector{
				Key:     r.usrWrap(last),
				OrEqual: true,
				Offset:  -1,
			},
		}
	} else {
		rng = &fdb.SelectorRange{
			Begin: &fdb.KeySelector{
				Key:     r.usrWrap(from),
				OrEqual: true,
				Offset:  1,
			},
			End: fdb.FirstGreaterOrEqual(r.endWrap(last)),
		}
	}

	return r.tx.GetRange(rng, fdb.RangeOptions{Limit: int(limit), Reverse: reverse})
}
