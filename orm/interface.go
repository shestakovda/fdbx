package orm

import (
	"context"

	"github.com/shestakovda/errors"
	"github.com/shestakovda/fdbx/mvcc"
)

type Collection interface {
	ID() uint16
	SysKey(mvcc.Key) mvcc.Key
	UsrKey(mvcc.Key) mvcc.Key
	NewRow(mvcc.Key, mvcc.Value) Row
}

type Table interface {
	Collection

	Select(mvcc.Tx) Query
	Upsert(mvcc.Tx, ...Model) error
}

type Query interface {
	ByID(id ...mvcc.Key) Query

	All(ctx context.Context) ([]Model, error)
	First(ctx context.Context) (Model, error)
	Delete(ctx context.Context) error

	Agg(ctx context.Context, funcs ...AggFunc) (err error)
}

type Model interface {
	Key() mvcc.Key
	Pack() (mvcc.Value, error)
	Unpack(mvcc.Value) error
}

type Row interface {
	Key() mvcc.Key
	Value() mvcc.Value
	Model() (Model, error)
}

type Selector interface {
	Select(context.Context, Collection) (<-chan Row, <-chan error)
}

type Filter interface {
	Skip(Row) (bool, error)
}

type AggFunc func(Row) error

type ModelFabric func(id mvcc.Key) Model

var (
	ErrSelect = errors.New("select")
	ErrUpsert = errors.New("upsert")
	ErrDelete = errors.New("delete")
	ErrStream = errors.New("stream")
	ErrFilter = errors.New("filter")

	ErrSelectByID = errors.New("select by ids")
	ErrSelectFull = errors.New("select full")
	ErrSelectAgg  = errors.New("select agg")

	ErrSelectAll   = errors.New("select all")
	ErrSelectFirst = errors.New("select first")

	ErrRowModel = errors.New("row model")
)
