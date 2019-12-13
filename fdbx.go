package fdbx

import (
	"context"
	"time"
)

// Supported FoundationDB client versions
const (
	ConnVersion610  = uint16(610)
	ConnVersionMock = uint16(0xFFFF)
)

const (
	// MaxChunkSize is max possible chunk size.
	MaxChunkSize = 100000
)

var (
	// CursorTypeID is collection number for storing cursors
	CursorTypeID = uint16(0)

	// ChunkTypeID is collection number for storing blob chunks. Default uint16 max value
	ChunkTypeID = uint16(0xFFFF)

	// ChunkSize is max chunk length. Default 100 Kb - fdb value limit
	ChunkSize = 100000

	// GZipSize is a value len more then GZipSize cause gzip processing
	GZipSize = 860
)

// TODO: agg funcs

// TxHandler -
type TxHandler func(DB) error

// IndexHandler -
type IndexHandler func(Indexer) error

// Predicat - for query filtering, especially for seq scan queries
type Predicat func(Record) (bool, error)

// Option - to describe additional args for select
type Option func(*options) error

// RecordType - to describe record collection
type RecordType struct {
	ID  uint16
	New func(id []byte) (Record, error)
}

// Conn - database connection (as stored database index)
type Conn interface {
	ClearDB() error

	Tx(TxHandler) error

	Queue(rtp RecordType, prefix []byte) (Queue, error)

	Cursor(rtp RecordType, start []byte, pageSize uint) (Cursor, error)
	LoadCursor(rtp RecordType, id []byte, pageSize uint) (Cursor, error)
}

// DB - database object that holds connection for transaction handler
type DB interface {
	Set(typeID uint16, id, value []byte) error
	Get(typeID uint16, id []byte) ([]byte, error)
	Del(typeID uint16, id []byte) error

	Save(...Record) error
	Load(...Record) error
	Drop(...Record) error

	Index(h IndexHandler, rid []byte, drop bool) error

	Select(rtp RecordType, opts ...Option) ([]Record, error)
}

// Cursor - helper for long seq scan queries or pagination
type Cursor interface {
	// cursor is saved to the database to eliminate transaction time limitation
	Record

	// if true, there are no records Next from cursor, but you can use Prev
	Empty() bool

	// mark cursor as empty and drop it from database
	Close() error

	// next or prev `page` records from collection or index
	Next(db DB, skip uint8) ([]Record, error)
	Prev(db DB, skip uint8) ([]Record, error)

	// select all records from current position to the end of collection
	Select(ctx context.Context, opts ...Option) (<-chan Record, <-chan error)
}

// Queue - async task manager (pub/sub) with persistent storage and processing delay
type Queue interface {
	// confirm task delivery
	Ack(db DB, ids ...[]byte) error

	// publish task with processing delay
	Pub(db DB, when time.Time, ids ...[]byte) error

	// subscriptions
	Sub(ctx context.Context) (<-chan Record, <-chan error)
	SubOne(ctx context.Context) (Record, error)
	SubList(ctx context.Context, limit uint) ([]Record, error)

	// unconfirmed (not Ack) tasks
	GetLost(limit uint, filter Predicat) ([]Record, error)
}

// Record - database record object (user model, collection item)
type Record interface {
	// object identifier in any format
	FdbxID() []byte
	// type identifier (collection id)
	FdbxType() RecordType
	// calc index values
	FdbxIndex(idx Indexer) error
	// make new buffer from object fields
	FdbxMarshal() ([]byte, error)
	// fill object fields from buffer
	FdbxUnmarshal([]byte) error
}

// Indexer - for record indexing
type Indexer interface {
	// append key for indexing as idxTypeID
	Index(idxTypeID uint16, value []byte)
}

// NewConn - makes new connection with specified client version
func NewConn(db, version uint16) (Conn, error) {
	// default 6.1.х
	if version == 0 {
		version = ConnVersion610
	}

	switch version {
	case ConnVersion610:
		return newV610Conn(db)
	case ConnVersionMock:
		return newMockConn(db)
	}

	return nil, ErrUnknownVersion
}
