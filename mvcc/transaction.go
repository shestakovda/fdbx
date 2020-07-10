package mvcc

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
	"github.com/shestakovda/fdbx/models"

	fbs "github.com/google/flatbuffers/go"
)

const setTx = 0

const (
	idxRunning  = 0
	idxFinished = 1
)

type transaction struct {
	uid    uuid.UUID
	txid   uint64
	start  uint64
	opmax  uint64
	cmmax  []byte
	opened []uint64
	status byte

	op *uint32
	db *database
}

func (tx *transaction) Get(setID, idxID byte, key []byte) (val []byte, err error) {

	if _, err = tx.db.root.ReadTransact(func(t fdb.ReadTransaction) (interface{}, error) {

		// Запрашиваем все версии этого объекта от начала времен и до последней закоммиченной транзакции
		// Такая планка сверху должна позволить:
		// 1. Сэкономить несколько строк на случай, когда значение часто изменяется
		// 2. Избавить нас от конфликтов с другими транзакциями, т.к. он добавляют "сверху"
		// Запрашиваем в обратном порядке, т.к. актуальная версия будет выше, сможем выйти из цикла раньше
		// Таким образом, сэкономим дополнительно несколько итераций по распаковке и проверке версий объектов
		rng := t.GetRange(fdb.KeyRange{
			Begin: tx.db.key(setID, idxID, key),
			End:   tx.db.key(setID, idxID, key, tx.cmmax),
		}, fdb.RangeOptions{Reverse: true, Mode: fdb.StreamingModeIterator}).Iterator()

		ccur := atomic.LoadUint32(tx.op)

		for rng.Advance() {
			row := newRow(rng.MustGet().Value)

			if (row.XMin() == tx.txid &&
				row.CMin() < ccur &&
				(row.XMax() == 0 ||
					(row.XMax() == tx.txid &&
						row.CMax() >= ccur))) ||
				(tx.isCommitted(row.XMin(), t) &&
					(row.XMax() == 0 ||
						row.CMax() >= ccur) ||
					row.XMax() != tx.txid &&
						!tx.isCommitted(row.XMax(), t)) {
				val = row.Value()
				return nil, nil
			}
		}

		return nil, nil
	}); err != nil {
		return
	}

	return nil, nil
}

func (tx *transaction) Set(setID, idxID byte, key, val []byte) error {
	return nil
}

func (tx *transaction) Commit() (err error) {

	if err = tx.close(txStatusCommitted); err != nil {
		return ErrCommit.WithReason(err)
	}

	return nil
}

func (tx *transaction) Rollback() (err error) {

	if err = tx.close(txStatusAborted); err != nil {
		return ErrRollback.WithReason(err)
	}

	return nil
}

func (tx *transaction) close(status txStatus) (err error) {
	var txid [8]byte
	binary.BigEndian.PutUint64(txid[:], tx.txid)

	// Упаковку и вычисление ключей можно сделать вне транзакции, чтобы сэкономить в случае повторов
	tx.status = status
	txData := tx.pack()
	runKey := tx.db.key(setTx, idxRunning, tx.uid[:])
	finKey := tx.db.key(setTx, idxFinished, txid)

	if _, err = tx.db.root.Transact(func(t fdb.Transaction) (interface{}, error) {
		// Удаляем из списка открытых транзакций
		t.Clear(runKey)

		// Сохраняем данные о транзакции в постоянное хранилище
		t.Set(finKey, txData)

		// Если закрывается успешно, увеличиваем значение максимальной успешной транзакции
		if status == txStatusCommitted {
			binary.LittleEndian.PutUint64(txid[:], tx.txid)
			t.Max(tx.db.key(setTx, idxFinished, max[:]), txid)
		}

		return nil, nil
	}); err != nil {
		return ErrClose.WithReason(err)
	}

	return nil
}

func (tx *transaction) pack() []byte {
	buf := fbs.NewBuilder(32)
	models.TransactionStart(buf)
	models.TransactionAddTxID(buf, tx.txid)
	models.TransactionAddStart(buf, tx.start)
	models.TransactionAddStatus(buf, tx.status)
	buf.Finish(models.TransactionEnd(buf))
	return buf.FinishedBytes()
}

func (tx *transaction) isCommitted(id uint64, t fdb.ReadTransaction) bool {
	var status txStatus

	if status = tx.db.txsc.get(id); status == txStatusUnknown {
		var txid [8]byte
		binary.BigEndian.PutUint64(txid[:], id)
		status = models.GetRootAsTransaction(t.Get(tx.db.key(setTx, idxFinished, txid)).MustGet(), 0).Status()
		tx.db.txsc.set(id, status)
	}

	return status == txStatusCommitted
}

func (tx *transaction) selectRow(rt fdb.ReadTransaction, key []byte) (_ *row, err error) {

	// Запрашиваем все существующие версии объекта, в т.ч. добавленные в конфликтующих транзакциях
	// Это очень важный момент, потому что так блокировка на уровне fdb не позволит при вставке появиться
	// нескольким версиям объекта, если вдруг они будут добавлены параллельно.
	// Запрашиваем в обратном порядке, в оптимистичном расчете, что актуальная версия скорее всего позднее
	versions := rt.GetRange(fdb.KeyRange{
		Begin: tx.db.key(setID, idxID, key),
		End:   tx.db.key(setID, idxID, key, end),
	}, fdb.RangeOptions{Reverse: true, Mode: fdb.StreamingModeWantAll}).GetSliceOrPanic()

	// Получаем номер текущей операции в транзакции
	ccur := atomic.LoadUint32(tx.op)

	// Ищем актуальную версию
	for i := range versions {
		row := newRow(versions[i].Value)

		if (row.XMin() == tx.txid &&
			row.CMin() < ccur &&
			(row.XMax() == 0 ||
				(row.XMax() == tx.txid &&
					row.CMax() >= ccur))) ||
			(tx.isCommitted(row.XMin(), t) &&
				(row.XMax() == 0 ||
					row.CMax() >= ccur) ||
				row.XMax() != tx.txid &&
					!tx.isCommitted(row.XMax(), t)) {
			return row, nil
		}
	}

	return nil
}

func (tx *transaction) insertRow() (err error) {
	return nil
}
