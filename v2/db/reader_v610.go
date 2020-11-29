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

func (r v610Reader) List(from, to fdbx.Key, limit uint64, reverse bool) fdbx.ListGetter {
	// В данном случае не передаем режим запроса, т.к. не оставляем это на выбор потребителя
	// Если вызывать GetSlice*, то будет StreamingModeWantAll или StreamingModeExact, зависит от наличия Limit
	// Если вызывать Iterator, то по-умолчанию будет последовательный режим StreamingModeIterator
	return &listGetter{
		r.tx.GetRange(&fdb.KeyRange{
			Begin: r.usrWrap(from).Raw(),
			End:   r.endWrap(to).Raw(),
		}, fdb.RangeOptions{
			Limit:   int(limit),
			Reverse: reverse,
		}),
	}
}

type listGetter struct {
	fdb.RangeResult
}

func (g listGetter) Resolve() []fdbx.Pair {
	list := g.GetSliceOrPanic()
	res := make([]fdbx.Pair, 0, len(list))

	for i := range list {
		res = append(res, fdbx.NewPair(
			fdbx.Bytes2Key(list[i].Key[1:]),
			list[i].Value,
		))
	}

	return res
}
