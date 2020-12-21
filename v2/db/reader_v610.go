package db

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/fdbx/v2"
)

var tail = bytes.Repeat([]byte{0xFF}, 256)

type v610Reader struct {
	v610Conn
	tx fdb.ReadTransaction
}

func (r v610Reader) usrWrap(key fdbx.Key) fdbx.Key {
	if key == nil {
		return fdbx.Bytes2Key(r.sk)
	}

	return key.LPart(r.sk...)
}

func (r v610Reader) endWrap(key fdbx.Key) fdbx.Key {
	if key == nil {
		return fdbx.Bytes2Key(r.ek)
	}

	return key.LPart(r.DB()).RPart(tail...)
}

// Получение объекта ожидания конкретного значения
func (r v610Reader) Data(key fdbx.Key) fdbx.Pair {
	wrk := r.usrWrap(key)
	return fdbx.NewPair(wrk, r.tx.Get(wrk.Raw()).MustGet())
}

func (r v610Reader) List(from, last fdbx.Key, limit uint64, reverse, skip bool) fdbx.ListGetter {
	rng := &fdb.SelectorRange{
		Begin: fdb.FirstGreaterOrEqual(r.usrWrap(from).Raw()),
		End:   fdb.FirstGreaterOrEqual(r.endWrap(last).Raw()),
	}

	if skip {
		if reverse {
			rng.End = &fdb.KeySelector{
				Key:     r.usrWrap(last).Raw(),
				OrEqual: true,
				Offset:  -1,
			}
		} else {
			rng.Begin = &fdb.KeySelector{
				Key:     r.usrWrap(from).Raw(),
				OrEqual: true,
				Offset:  1,
			}
		}
	}

	// В данном случае не передаем режим запроса, т.к. не оставляем это на выбор потребителя
	// Если вызывать GetSlice*, то будет StreamingModeWantAll или StreamingModeExact, зависит от наличия Limit
	// Если вызывать Iterator, то по-умолчанию будет последовательный режим StreamingModeIterator
	return &listGetter{RangeResult: r.tx.GetRange(rng, fdb.RangeOptions{Limit: int(limit), Reverse: reverse})}
}

type listGetter struct {
	fdb.RangeResult
	res []fdbx.Pair
}

func (g *listGetter) Resolve() []fdbx.Pair {
	if g.res == nil {
		list := g.GetSliceOrPanic()
		g.res = make([]fdbx.Pair, len(list))

		for i := range list {
			g.res[i] = fdbx.NewPair(fdbx.Bytes2Key(list[i].Key[1:]), list[i].Value)
		}
	}

	return g.res
}
