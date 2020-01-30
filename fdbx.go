package fdbx

import (
	"context"
	"time"
	"unsafe"
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

//nolint:gochecknoglobals
var (
	// CursorTypeID is collection number for storing cursors
	CursorTypeID = uint16(0xFFFD)

	// CursorIndexID is collection number for storing cursor created index
	CursorIndexID = uint16(0xFFFE)

	// ChunkTypeID is collection number for storing blob chunks. Default uint16 max value
	ChunkTypeID = uint16(0xFFFF)

	// ChunkSize is max chunk length. Default 100 Kb - fdb value limit
	ChunkSize = 100000

	// GZipSize is a value len more then GZipSize cause gzip processing
	GZipSize = 860

	// PunchSize - poll wait interval for queue when no new tasks registered
	PunchSize = time.Minute
)

const (
	flagVersion = uint8(1 << 5)
	flagGZip    = uint8(1 << 6)
	flagChunk   = uint8(1 << 7)
)

// TaskStatus - alias
type TaskStatus uint8

// task status
const (
	StatusPublished   TaskStatus = 1
	StatusUnconfirmed TaskStatus = 2
	StatusConfirmed   TaskStatus = 3
)

// TxHandler -
type TxHandler func(DB) error

// IndexHandler -
type IndexHandler func(Indexer) error

// RecordHandler -
type RecordHandler func(Record) error

// RecordFabric -
type RecordFabric func(ver uint8, id string) (Record, error)

// Condition - for query filtering, especially for seq scan queries
type Condition func(Record) (bool, error)

// Option - to describe additional args for select
type Option func(*options) error

// RecordType - to describe record collection
type RecordType struct {
	ID  uint16
	Ver uint8
	New RecordFabric
}

// Conn - database connection (as stored database index)
type Conn interface {
	ClearDB() error

	Tx(TxHandler) error

	Queue(rtp RecordType, prefix string) (Queue, error)

	Cursor(rtp RecordType, opts ...Option) (Cursor, error)
	LoadCursor(id string, rf RecordFabric, opts ...Option) (Cursor, error)

	CursorID(rtp RecordType, opts ...Option) (CursorID, error)

	StartClearDaemon()
}

// DB - database object that holds connection for transaction handler
type DB interface {
	Set(typeID uint16, id, value []byte) error
	Get(typeID uint16, id []byte) ([]byte, error)
	Del(typeID uint16, id []byte) error

	Save(onExists RecordHandler, recs ...Record) error
	Load(onNotFound RecordHandler, recs ...Record) error
	Drop(onNotExists RecordHandler, recs ...Record) error

	Index(h IndexHandler, rid string, drop bool) error

	Clear(typeID uint16) error
	ClearIndex(h IndexHandler) error

	Select(rtp RecordType, opts ...Option) ([]Record, error)
	SelectIDs(typeID uint16, opts ...Option) ([]string, error)
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

// CursorID - helper for long seq scan queries or pagination
type CursorID interface {
	// if true, there are no records Next from cursor, but you can use Prev
	Empty() bool

	// mark cursor as empty and drop it from database
	Close() error

	// next or prev `page` records from collection or index
	Next(db DB, skip uint8) ([]string, error)
	Prev(db DB, skip uint8) ([]string, error)

	// select all records from current position to the end of collection
	Select(ctx context.Context) (<-chan string, <-chan error)
}

// Queue - async task manager (pub/sub) with persistent storage and processing delay
type Queue interface {
	// confirm task delivery
	Ack(db DB, ids ...string) error

	// publish task with processing delay
	Pub(db DB, when time.Time, ids ...string) error

	// subscriptions
	Sub(ctx context.Context) (<-chan Record, <-chan error)
	SubOne(ctx context.Context) (Record, error)
	SubList(ctx context.Context, limit uint) ([]Record, error)

	// unconfirmed (not Ack) tasks
	GetLost(limit uint, cond Condition) ([]Record, error)

	// queue counts
	Stat() (wait, lost int, err error)

	// task status
	Status(db DB, ids ...string) (map[string]TaskStatus, error)
}

// Record - database record object (user model, collection item)
type Record interface {
	// object identifier in any format
	FdbxID() string
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
	// Grow inner buffers to decrease allocs
	Grow(n int)
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
		return new(MockConn), nil
	}

	return nil, ErrUnknownVersion
}

// S2B - fast and dangerous!
func S2B(s string) []byte {
	if s == "" {
		return nil
	}
	return *(*[]byte)(unsafe.Pointer(&s))
}

// S2B - fast and dangerous!
func B2S(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return *(*string)(unsafe.Pointer(&b))
}
