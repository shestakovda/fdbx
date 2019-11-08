package fdbx

import (
	"time"
)

// TODO: agg funcs

// Cursor -
type Cursor interface {
	Model
	LastKey() []byte
	Created() time.Time
}
