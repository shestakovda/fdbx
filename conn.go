package fdbx

import "github.com/apple/foundationdb/bindings/go/src/fdb"

// Supported FoundationDB client versions
const (
	ConnVersionMock = 0xFFFF
	ConnVersion610  = 610
)

// TxHandler -
type TxHandler func(DB) error

// Index - calc index key from model buffer
type Index func([]byte) (fdb.Key, error)

// Fabric - model fabric func
type Fabric func(id []byte) (Model, error)

// Conn - database connection (as stored database index)
type Conn interface {
	DB() uint16
	Key(ctype uint16, id []byte) (fdb.Key, error)
	MKey(Model) (fdb.Key, error)

	ClearDB() error
	Tx(TxHandler) error

	Indexes(ctype uint16) []Index
	AddIndex(ctype uint16, index Index)

	Queue(qtype uint16, f Fabric) Queue
}

// NewConn - makes new connection with specified client version
func NewConn(db, version uint16) (Conn, error) {
	// default 6.1.х
	if version == 0 {
		version = ConnVersion610
	}

	switch version {
	case ConnVersionMock:
		return newMockConn(db)
	case ConnVersion610:
		return newV610Conn(db)
	}

	return nil, ErrUnknownVersion
}
