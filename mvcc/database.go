package mvcc

import (
	"encoding/binary"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
)

const end = [1]byte{0xFF}

type database struct {
	id   byte
	root fdb.Database
}

func (db *database) Begin() (_ Tx, err error) {
	tx := &transaction{
		start: uint64(time.Now().UTC().UnixNano()),
		uid:   uuid.New(),
		db:    db,
	}

	if _, err = db.root.Transact(func(t fdb.Transaction) (interface{}, error) {
		var value [22]byte

		// Запрашиваем список транзакций, открытых на данный момент. Нам нужны их номера
		list := t.GetRange(fdb.KeyRange{
			Begin: db.key(setTx, idxRunning),
			Begin: db.key(setTx, idxRunning, end[:]),
		}, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).GetSliceOrPanic()

		tx.opened = make([]uint64, len(list))
		for i := range list {
			tx.opened[i] = binary.BigEndian.Uint64(list[i].Value[:8])
		}

		// Добавляем в список открытых транзакций новую. Её идентификатор еще не знаем,
		// Он будет сформирован в момент коммита этой транзакции и записан вместо value
		// Попутно в value записываем момент начала транзакции, чтобы vacuum мог удалять
		// зависшие (мало ли, в процессе работы вылетит паника или просто вырубят свет)
		binary.BigEndian.PutUint64(value[10:18], tx.start)
		t.SetVersionstampedValue(db.key(setTx, idxRunning, tx.uid[:]), value[:])
		return nil, nil
	}); err != nil {
		return ErrBegin.WithReason(err)
	}

	return tx, nil
}

func (db *database) key(setID, idxID byte, parts ...[]byte) fdb.Key {
	mem := 3
	ptr := 3
	plen := 0

	for i := range parts {
		mem += len(parts[i])
	}

	key := make(fdb.Key, mem)
	key[0] = db.id
	key[1] = setID
	key[2] = idxID

	for i := range parts {
		if plen = len(parts[i]); plen > 0 {
			copy(key[ptr:], parts[i])
			ptr += plen
		}
	}

	return key
}
