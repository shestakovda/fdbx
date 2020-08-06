package mvcc

import (
	"context"

	"github.com/shestakovda/errors"
)

type Pair struct {
	Key   Key
	Value Value
}

type Range struct {
	From Key
	To   Key
}

type Key interface {
	Bytes() []byte
	String() string
}

type Value interface {
	Bytes() []byte
	String() string
}

type Tx interface {
	Commit() error
	Cancel() error

	Delete(Key) error
	Upsert(Key, Value) error
	Select(Key) (Value, error)

	SeqScan(ctx context.Context, rng *Range) (<-chan *Pair, <-chan error)
	FullScan(ctx context.Context, rng *Range, workers int) (<-chan *Pair, <-chan error)
}

var (
	ErrBegin  = errors.New("begin")
	ErrSelect = errors.New("select")
	ErrUpsert = errors.New("upsert")
	ErrDelete = errors.New("delete")
	ErrCommit = errors.New("commit")
	ErrCancel = errors.New("cancel")

	ErrFullScan     = errors.New("full scan")
	ErrFullScanRead = errors.New("full scan read")

	ErrSubRanges = errors.New("sub ranges")

	ErrFetchTx  = errors.New("fetch tx")
	ErrFetchRow = errors.New("fetch row")

	ErrDropTx  = errors.New("drop tx")
	ErrDropRow = errors.New("drop row")

	ErrSaveTx  = errors.New("save tx")
	ErrSaveRow = errors.New("save row")

	ErrCloseTx = errors.New("close tx")
)
