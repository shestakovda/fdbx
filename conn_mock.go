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
	FSave     func(...Record) error
	FLoad     func(...Record) error
	FDrop     func(...Record) error
	FSelect   func(ctype uint16, fab Fabric, opts ...Option) ([]Record, error)

	// ***** Queue *****

	FAck      func(DB, Record) error
	FPub      func(DB, Record, time.Time) error
	FSub      func(ctx context.Context) (<-chan Record, <-chan error)
	FSubOne   func(ctx context.Context) (Record, error)
	FSubList  func(ctx context.Context, limit int) ([]Record, error)
	FGetLost  func(limit int) ([]Record, error)
	FSettings func() (uint16, Fabric)
}

// ClearDB - clear stub, set FClearDB before usage
func (c *MockConn) ClearDB() error { return c.FClearDB() }

// Tx - tx stub, create MockDB object
func (c *MockConn) Tx(h TxHandler) error { return h(newMockDB(c)) }

// Queue - clear stub, set FQueue before usage
func (c *MockConn) Queue(qtype uint16, f Fabric) (Queue, error) { return newMockQueue(c, qtype, f) }
