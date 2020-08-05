package mvcc

import "github.com/shestakovda/errors"

type Key interface {
	Bytes() []byte
	String() string
}

type Value interface {
	Bytes() []byte
	String() string
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

	ErrDropTx  = errors.New("drop tx")
	ErrDropRow = errors.New("drop row")

	ErrSaveTx  = errors.New("save tx")
	ErrSaveRow = errors.New("save row")

	ErrCloseTx = errors.New("close tx")
)
