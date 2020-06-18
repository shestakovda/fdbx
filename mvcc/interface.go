package mvcc

import "github.com/shestakovda/errors"

type DB interface {
	Begin() (Tx, error)
}

type Tx interface {
	Commit() error
	Rollback()
}

var (
	ErrBegin    = errors.New("can't create a transaction")
	ErrClose    = errors.New("can't close a transaction")
	ErrCommit   = errors.New("can't commit a transaction")
	ErrRollback = errors.New("can't abort a transaction")
)
