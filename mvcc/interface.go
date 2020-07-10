package mvcc

import "github.com/shestakovda/errors"

type Key interface {
	Bytes() []byte
}

type Value interface {
}

type Tx interface {
	Select(Key) (Value, error)
	// Upsert(key, val []byte) error
	// Delete(key []byte) error
	// Commit() error
	// Cancel()
}

var (
	ErrBegin  = errors.New("begin error")
	ErrSelect = errors.New("select error")

// ErrClose    = errors.New("tx close error")
// ErrCommit   = errors.New("tx commit error")
// ErrRollback = errors.New("tx rollback error")
)
