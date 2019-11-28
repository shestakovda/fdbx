package fdbx

import (
	"context"
	"time"
)

func newMockConn(db uint16) (conn *MockConn, err error) {
	return &MockConn{baseConn: newBaseConn(db)}, nil
}

// MockConn - stub conn for unit testing
type MockConn struct {
	*baseConn

	FClearDB func() error
	FQueue   func(uint16, Fabric) Queue

	// ***** DB *****

	FClearAll func() error
	FSet      func(ctype uint16, id, value []byte) error
	FGet      func(ctype uint16, id []byte) ([]byte, error)
	FDel      func(ctype uint16, id []byte) error
	FSave     func(...Model) error
	FLoad     func(...Model) error
	FDrop     func(...Model) error
	FSelect   func(ctype uint16, fab Fabric, opts ...Option) ([]Model, error)

	// ***** Queue *****

	FAck      func(DB, Model) error
	FPub      func(DB, Model, time.Time) error
	FSub      func(ctx context.Context) (<-chan Model, <-chan error)
	FSubOne   func(ctx context.Context) (Model, error)
	FSubList  func(ctx context.Context, limit int) ([]Model, error)
	FGetLost  func(limit int) ([]Model, error)
	FSettings func() (uint16, Fabric)
}

// ClearDB - clear stub, set FClearDB before usage
func (c *MockConn) ClearDB() error { return c.FClearDB() }

// Tx - tx stub, create MockDB object
func (c *MockConn) Tx(h TxHandler) error { return h(newMockDB(c)) }

// Queue - clear stub, set FQueue before usage
func (c *MockConn) Queue(qtype uint16, f Fabric) Queue { return newMockQueue(c, qtype, f) }
