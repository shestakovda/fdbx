package fdbx

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newV610Conn(db uint16) (conn *v610Conn, err error) {
	conn = &v610Conn{db: db}

	// Installed version check
	if err = fdb.APIVersion(ConnVersion610); err != nil {
		return nil, ErrOldVersion.WithReason(err)
	}

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

func (c *v610Conn) Key(ctype uint16, id []byte) ([]byte, error) {
	if len(id) == 0 {
		return nil, ErrEmptyID
	}

	// TODO: using sync.Pool
	idl := len(id)
	key := make([]byte, 4+idl)

	// zero values is supported
	binary.BigEndian.PutUint16(key[0:2], c.db)
	binary.BigEndian.PutUint16(key[2:4], ctype)

	// just for convenience
	if n := copy(key[4:], id); n != idl {
		return nil, ErrMemFail
	}

	return key, nil
}

func (c *v610Conn) MKey(m Model) ([]byte, error) {
	if m == nil {
		return nil, ErrNullModel
	}

	return c.Key(m.Type(), m.ID())
}

func (c *v610Conn) Tx(h TxHandler) error {
	_, exp := c.fdb.Transact(func(tx fdb.Transaction) (_ interface{}, err error) {
		var db DB

		if db, err = newV610db(c, tx); err != nil {
			return
		}

		return nil, h(db)
	})
	return exp
}
