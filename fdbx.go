package fdbx

import (
	"time"
)

// Predict - for query filtering, specially for seq scan queries
type Predict func(buf []byte) (bool, error)

// TODO: agg funcs

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
