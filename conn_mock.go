package fdbx

import (
	"context"
	"time"
)

func newMockConn(db uint16) (conn *MockConn, err error) {
	return &MockConn{db: db}, nil
}

// MockConn - stub conn for unit testing
type MockConn struct {
	db uint16

	FClearDB    func() error
	FLoadCursor func(rtp RecordType, id []byte, page uint) (Cursor, error)

	// ***** DB *****

	FClearAll  func() error
	FSet       func(ctype uint16, id, value []byte) error
	FGet       func(ctype uint16, id []byte) ([]byte, error)
	FDel       func(ctype uint16, id []byte) error
	FSave      func(RecordHandler, ...Record) error
	FLoad      func(RecordHandler, ...Record) error
	FDrop      func(RecordHandler, ...Record) error
	FIndex     func(IndexHandler, []byte, bool) error
	FSelect    func(rtp RecordType, opts ...Option) ([]Record, error)
	FSelectIDs func(typeID uint16, opts ...Option) ([][]byte, error)

	// ***** Queue *****

	FAck       func(DB, ...[]byte) error
	FPub       func(DB, time.Time, ...[]byte) error
	FSub       func(ctx context.Context) (<-chan Record, <-chan error)
	FSubOne    func(ctx context.Context) (Record, error)
	FSubList   func(ctx context.Context, limit uint) ([]Record, error)
	FGetLost   func(limit uint, filter Predicat) ([]Record, error)
	FCheckLost func(db DB, ids ...[]byte) ([]bool, error)
	FStat      func() (wait, lost int, err error)

	// ***** Cursor *****

	FEmpty        func() bool
	FClose        func() error
	FNext         func(db DB, skip uint8) ([]Record, error)
	FPrev         func(db DB, skip uint8) ([]Record, error)
	FCursorSelect func(ctx context.Context) (<-chan Record, <-chan error)

	// ***** Record *****

	FFdbxID        func() []byte
	FFdbxType      func() RecordType
	FFdbxIndex     func(Indexer) error
	FFdbxMarshal   func() ([]byte, error)
	FFdbxUnmarshal func([]byte) error
}

// ClearDB - clear stub, set FClearDB before usage
func (c *MockConn) ClearDB() error { return c.FClearDB() }

// Tx - tx stub, create mock object
func (c *MockConn) Tx(h TxHandler) error { return h(newMockDB(c)) }

// Queue - queue stub, create mock object
func (c *MockConn) Queue(rtp RecordType, prefix []byte) (Queue, error) {
	return newMockQueue(c, rtp, prefix)
}

// Cursor - cursor stub, create mock object
func (c *MockConn) Cursor(rtp RecordType, start []byte, page uint) (Cursor, error) {
	return newMockCursor(c, rtp, start, page)
}

// LoadCursor - load cursor stub
func (c *MockConn) LoadCursor(rtp RecordType, id []byte, page uint) (Cursor, error) {
	return c.FLoadCursor(rtp, id, page)
}
