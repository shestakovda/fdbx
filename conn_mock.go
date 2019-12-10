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

	FClearDB    func() error
	FLoadCursor func(id []byte, page int) (Cursor, error)

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

	// ***** Cursor *****

	FEmpty        func() bool
	FClose        func() error
	FNext         func(db DB, skip uint8) ([]Record, error)
	FPrev         func(db DB, skip uint8) ([]Record, error)
	FCursorSelect func(ctx context.Context) (<-chan Record, <-chan error)

	// ***** Record *****

	FFdbxID        func() []byte
	FFdbxType      func() uint16
	FFdbxMarshal   func() ([]byte, error)
	FFdbxUnmarshal func([]byte) error
}

// ClearDB - clear stub, set FClearDB before usage
func (c *MockConn) ClearDB() error { return c.FClearDB() }

// Tx - tx stub, create mock object
func (c *MockConn) Tx(h TxHandler) error { return h(newMockDB(c)) }

// Queue - queue stub, create mock object
func (c *MockConn) Queue(typeID uint16, fab Fabric) (Queue, error) {
	return newMockQueue(c, typeID, fab)
}

// Cursor - cursor stub, create mock object
func (c *MockConn) Cursor(typeID uint16, fab Fabric, start []byte, page int) (Cursor, error) {
	return newMockCursor(c, typeID, fab, start, page)
}

// LoadCursor - load cursor stub
func (c *MockConn) LoadCursor(id []byte, page int) (Cursor, error) {
	return c.FLoadCursor(id, page)
}
