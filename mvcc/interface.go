package mvcc

import (
	"github.com/shestakovda/errors"
	"github.com/shestakovda/fdbx/db"
)

type Key interface {
	Bytes() []byte
	Namespace() db.Namespace
}

type Value interface {
	Bytes() []byte
}

type Tx interface {
	Select(Key) (Value, error)
	Upsert(Key, Value) error
	Delete(Key) error
	Commit() error
	Cancel() error
}

var (
	ErrBegin  = errors.New("begin")
	ErrSelect = errors.New("select")
	ErrUpsert = errors.New("upsert")
	ErrDelete = errors.New("delete")
	ErrCommit = errors.New("commit")
	ErrCancel = errors.New("cancel")

	ErrFetchTx  = errors.New("fetch tx")
	ErrFetchRow = errors.New("fetch row")

	ErrDropRow = errors.New("drop row")

	ErrSaveTx  = errors.New("save tx")
	ErrSaveRow = errors.New("save row")

	ErrCloseTx = errors.New("close tx")
)
