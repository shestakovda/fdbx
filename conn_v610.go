package fdbx

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newV610Conn(db uint16) (*v610Conn, error) {
	var err error

	// Installed version check
	if err = fdb.APIVersion(ConnVersion610); err != nil {
		return nil, ErrOldVersion.WithReason(err)
	}

	conn := &v610Conn{db: db}

	// Default from /etc/foundationdb/fdb.cluster
	if conn.fdb, err = fdb.OpenDefault(); err != nil {
		return nil, ErrConnect.WithReason(err)
	}

	return conn, nil
}

type v610Conn struct {
	db  uint16
	fdb fdb.Database
}

func (c *v610Conn) Key(m Model) ([]byte, error) {
	var id []byte

	if m == nil {
		return nil, ErrNullModel
	}

	if id = m.ID(); len(id) == 0 {
		return nil, ErrEmptyModelID
	}

	// TODO: using sync.Pool
	idl := len(id)
	key := make([]byte, 4+idl)

	// zero values is supported
	binary.LittleEndian.PutUint16(key[0:2], c.db)
	binary.LittleEndian.PutUint16(key[2:4], m.Type())

	// just for convenience
	if n := copy(key[4:], id); n != idl {
		return nil, ErrMemFail
	}

	return key, nil
}

func (c *v610Conn) Tx(h TxHandler) error {
	return nil
}
