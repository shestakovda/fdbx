package orm

import (
	"context"

	"github.com/shestakovda/errors"
	"github.com/shestakovda/fdbx/mvcc"
)

type Collection interface {
	ID() uint16
	Fabric() ModelFabric

	SysKey(usr mvcc.Key) mvcc.Key
	UsrKey(sys mvcc.Key) mvcc.Key

	Upsert(mvcc.Tx, Model) error

	Select(mvcc.Tx) Query
}

type Query interface {
	ByID(id ...mvcc.Key) Query

	All(ctx context.Context) ([]Model, error)
	First(ctx context.Context) (Model, error)
	Delete(ctx context.Context) error
}

type Model interface {
	Key() mvcc.Key
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

	ErrSelectAll   = errors.New("select all")
	ErrSelectFirst = errors.New("select first")
)
