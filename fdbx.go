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
	// CursorType is collection number for storing cursors
	CursorType = uint16(0)

	// ChunkType is collection number for storing blob chunks. Default uint16 max value
	ChunkType = uint16(0xFFFF)

	// ChunkSize is max chunk length. Default 100 Kb - fdb value limit
	ChunkSize = 100000

	// GZipSize is a value len more then GZipSize cause gzip processing
	GZipSize = 860
)

// TODO: agg funcs

// TxHandler -
type TxHandler func(DB) error

// IndexFunc - calc index keys from model buffer
type IndexFunc func(idx Indexer, buf []byte) error

// Fabric - model fabric func
type Fabric func(id []byte) (Record, error)

// Predicat - for query filtering, especially for seq scan queries
type Predicat func(Record) (bool, error)

// Option -
type Option func(*options) error

// Conn - database connection (as stored database index)
type Conn interface {
	ClearDB() error

	RegisterIndex(recordTypeID uint16, idxFunc IndexFunc)

	Tx(TxHandler) error

	Queue(typeID uint16, f Fabric) (Queue, error)

	Cursor(typeID uint16, f Fabric, start []byte, pageSize int) (Cursor, error)
	LoadCursor(id []byte, pageSize int) (Cursor, error)
}

// DB - database object that holds connection for transaction handler
type DB interface {
	Set(typeID uint16, id, value []byte) error
	Get(typeID uint16, id []byte) ([]byte, error)
	Del(typeID uint16, id []byte) error

	Save(...Record) error
	Load(...Record) error
	Drop(...Record) error

	Select(indexID uint16, fab Fabric, opts ...Option) ([]Record, error)
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
	Select(ctx context.Context) (<-chan Record, <-chan error)
}

// Queue -
type Queue interface {
	Ack(DB, Record) error

	Pub(DB, Record, time.Time) error

	Sub(ctx context.Context) (<-chan Record, <-chan error)
	SubOne(ctx context.Context) (Record, error)
	SubList(ctx context.Context, limit int) ([]Record, error)

	GetLost(limit int) ([]Record, error)
}

// Record - database record object (user model, collection item)
type Record interface {
	// object identifier in any format
	FdbxID() []byte
	// type identifier (collection id)
	FdbxType() uint16
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
