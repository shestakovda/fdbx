package orm

import (
	"context"

	"github.com/shestakovda/errors"
	"github.com/shestakovda/fdbx/mvcc"
)

type Collection interface {
	ID() uint16
	Fabric() ModelFabric

	ID2Key(mvcc.Key) mvcc.Key
	Key2ID(mvcc.Key) mvcc.Key

	Upsert(mvcc.Tx, Model) error

	Select(mvcc.Tx) Query
}

type Query interface {
	ByID(id ...mvcc.Key) Query

	First() (Model, error)

	Delete() error
}

type Model interface {
	ID() mvcc.Key

	Pack() (mvcc.Value, error)
	Unpack(mvcc.Value) error
}

type Selector interface {
	Select(context.Context, Collection) (<-chan Model, <-chan error)
}

type Filter interface {
	Skip(Model) (bool, error)
}

type ModelFabric func(id mvcc.Key) Model

var (
	ErrSelect = errors.New("select")
	ErrUpsert = errors.New("upsert")
	ErrDelete = errors.New("delete")
	ErrStream = errors.New("stream")
	ErrFilter = errors.New("filter")

	ErrSelectByID = errors.New("select by ids")
	ErrSelectFull = errors.New("select full")
)
