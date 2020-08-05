package orm

import (
	"github.com/shestakovda/errors"
	"github.com/shestakovda/fdbx/mvcc"
)

type Collection interface {
	ID() uint16

	Upsert(mvcc.Tx, Model) error

	Select(mvcc.Tx) Query
}

type Query interface {
	ByID(id ...string) Query

	First() (Model, error)

	Delete() error
}

type Model interface {
	ID() mvcc.Key

	Pack() (mvcc.Value, error)
	Unpack(mvcc.Value) error
}

type Selector interface {
	Select() (<-chan Model, <-chan error)
}

type Filter interface {
	Skip(Model) (bool, error)
}

var (
	ErrSelect = errors.New("select")
	ErrUpsert = errors.New("upsert")
	ErrDelete = errors.New("delete")
	ErrStream = errors.New("stream")
	ErrFilter = errors.New("filter")
)
