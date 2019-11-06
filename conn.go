package fdbx

// Supported FoundationDB client versions
const (
	ConnVersion610 = 610
)

// TxHandler -
type TxHandler func(DB) error

// Index - calc index key from model buffer
type Index func([]byte) ([]byte, error)

// Conn - database connection (as stored database index)
type Conn interface {
	DB() uint16
	Key(ctype uint16, id []byte) ([]byte, error)
	MKey(Model) ([]byte, error)

	Tx(TxHandler) error

	Indexes(ctype uint16) []Index
	AddIndex(ctype uint16, index Index)
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
	}

	return nil, ErrUnknownVersion
}
