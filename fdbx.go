package fdbx

import (
	"context"
	"time"
)

// Predict - for query filtering, specially for seq scan queries
type Predict func(buf []byte) (bool, error)

// Option -
type Option func(*options) error

// TODO: agg funcs

// TxHandler -
type TxHandler func(DB) error

// Model -
type Model interface {
	ID() []byte
	Type() uint16
	Load([]byte)
	Save() []byte
	Dump() []byte
}

// Cursor -
type Cursor interface {
	Model
	LastKey() []byte
	Created() time.Time
}

// Queue -
type Queue interface {
	Ack(Model) error
	Pub(Model, time.Time) error
	Sub() (<-chan Model, error)
	Lost() ([]Model, error)
}

// DB -
type DB interface {
	Get(ctype uint16, id []byte) ([]byte, error)
	Set(ctype uint16, id, value []byte) error

	Load(...Model) error
	Save(...Model) error

	Select(ctx context.Context, ctype uint16, c Cursor, opts ...Option) (<-chan Model, error)
}
