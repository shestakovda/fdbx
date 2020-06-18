package mvcc

import (
	"encoding/binary"

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

type txStatus byte

const (
	txStatusUnknown   txStatus = 0
	txStatusAborted   txStatus = 1
	txStatusCommitted txStatus = 2
)

type transaction struct {
	uid    uuid.UUID
	txid   uint64
	start  uint64
	opened []uint64
	status byte

	db *database
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

	tx.status = status
	txData := tx.pack()
	runKey := tx.db.key(setTx, idxRunning, tx.uid[:])
	finKey := tx.db.key(setTx, idxFinished, txid)

	if _, err = tx.db.root.Transact(func(t fdb.Transaction) (interface{}, error) {
		t.Clear(runKey)
		t.Set(finKey, txData)
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
	models.TransactionAddCommitted(buf, tx.status)
	buf.Finish(models.TransactionEnd(buf))
	return buf.FinishedBytes()
}
