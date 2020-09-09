package db

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/fdbx"
)

var tail = bytes.Repeat([]byte{0xFF}, 256)

type v610Reader struct {
	v610Conn
	tx fdb.ReadTransaction
}

func (r v610Reader) usrWrapper(key fdbx.Key) fdbx.Key {
	if key == nil {
		return r.sk
	}

	return key.LPart(r.sk...)
}

func (r v610Reader) endWrapper(key fdbx.Key) fdbx.Key {
	if key == nil {
		return r.ek
	}

	return key.LPart(r.DB()).RPart(tail...)
}

// Получение объекта ожидания конкретного значения
func (r v610Reader) Data(key fdbx.Key) fdbx.Pair {
	return fdbx.NewPair(key, r.tx.Get(r.usrWrapper(key).Bytes()).MustGet())
}

func (r v610Reader) List(from, to fdbx.Key, limit uint64, reverse bool) ListGetter {
	// В данном случае не передаем режим запроса, т.к. не оставляем это на выбор потребителя
	// Если вызывать GetSlice*, то будет StreamingModeWantAll или StreamingModeExact, зависит от наличия Limit
	// Если вызывать Iterator, то по-умолчанию будет последовательный режим StreamingModeIterator
	fut := r.tx.GetRange(fdb.KeyRange{
		Begin: r.usrWrapper(from).Bytes(),
		End:   r.endWrapper(to).Bytes(),
	}, fdb.RangeOptions{
		Limit:   int(limit),
		Reverse: reverse,
	})

	return func() []fdbx.Pair {
		list := fut.GetSliceOrPanic()
		res := make([]fdbx.Pair, len(list))

		for i := range list {
			res[i] = fdbx.NewPair(fdbx.Key(list[i].Key).LSkip(1), list[i].Value)
		}

		return res
	}
}
