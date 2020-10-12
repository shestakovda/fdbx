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

func (r v610Reader) usrWrapper(key fdbx.Key) (fdbx.Key, error) {
	if key == nil {
		return r.sk, nil
	}

	return key.LPart(r.sk...), nil
}

func (r v610Reader) endWrapper(key fdbx.Key) (fdbx.Key, error) {
	if key == nil {
		return r.ek, nil
	}

	return key.LPart(r.DB()).RPart(tail...), nil
}

// Получение объекта ожидания конкретного значения
func (r v610Reader) Data(key fdbx.Key) (_ fdbx.Pair, err error) {
	var wrk fdbx.Key

	if wrk, err = r.usrWrapper(key); err != nil {
		return
	}

	return fdbx.NewPair(key, r.tx.Get(wrk.Bytes()).MustGet()), nil
}

func (r v610Reader) List(from, to fdbx.Key, limit uint64, reverse bool) (_ ListGetter, err error) {
	var uwk, ewk fdbx.Key

	if uwk, err = r.usrWrapper(from); err != nil {
		return
	}

	if ewk, err = r.endWrapper(to); err != nil {
		return
	}

	// В данном случае не передаем режим запроса, т.к. не оставляем это на выбор потребителя
	// Если вызывать GetSlice*, то будет StreamingModeWantAll или StreamingModeExact, зависит от наличия Limit
	// Если вызывать Iterator, то по-умолчанию будет последовательный режим StreamingModeIterator
	fut := r.tx.GetRange(fdb.KeyRange{
		Begin: uwk.Bytes(),
		End:   ewk.Bytes(),
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
	}, nil
}
