package fdbx

import (
	"encoding/binary"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newV610Conn(db uint16) (conn *v610Conn, err error) {
	conn = &v610Conn{
		db:      db,
		indexes: make(map[uint16][]Index, 8),
	}

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
	sync.RWMutex
	db      uint16
	fdb     fdb.Database
	indexes map[uint16][]Index
}

func (c *v610Conn) Key(ctype uint16, id []byte) ([]byte, error) {
	if len(id) == 0 {
		return nil, ErrEmptyID
	}

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

func (c *v610Conn) Indexes(ctype uint16) []Index {
	c.RLock()
	defer c.RUnlock()

	return c.indexes[ctype]
}

func (c *v610Conn) AddIndex(ctype uint16, index Index) {
	c.Lock()
	defer c.Unlock()
	c.indexes[ctype] = append(c.indexes[ctype], index)
}
