package fdbx

import (
	"context"
	"time"
)

// Queue -
type Queue interface {
	Ack(DB, Model) error

	Pub(DB, Model, time.Time) error

	Sub(ctx context.Context) (<-chan Model, <-chan error)
	SubOne(ctx context.Context) (Model, error)
	SubList(ctx context.Context, limit int) ([]Model, error)

	GetLost(limit int) ([]Model, error)
}
