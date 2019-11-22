package fdbx

import (
	"encoding/binary"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newBaseConn(db uint16) *baseConn {
	return &baseConn{
		db:      db,
		indexes: make(map[uint16][]Index, 8),
	}
}

type baseConn struct {
	sync.RWMutex
	db      uint16
	indexes map[uint16][]Index
}

func (c *baseConn) DB() uint16 { return c.db }

func (c *baseConn) Key(ctype uint16, id []byte) (fdb.Key, error) {
	if len(id) == 0 {
		return nil, ErrEmptyID.WithStack()
	}

	key := make(fdb.Key, 4+len(id))

	// zero values is supported
	binary.BigEndian.PutUint16(key[0:2], c.db)
	binary.BigEndian.PutUint16(key[2:4], ctype)

	if n := copy(key[4:], id); n != len(id) {
		return nil, ErrMemFail.WithStack()
	}

	return key, nil
}

func (c *baseConn) MKey(m Model) (fdb.Key, error) {
	if m == nil {
		return nil, ErrNullModel.WithStack()
	}

	return c.Key(m.Type(), m.ID())
}

func (c *baseConn) Indexes(ctype uint16) []Index {
	c.RLock()
	defer c.RUnlock()
	return c.indexes[ctype]
}

func (c *baseConn) AddIndex(ctype uint16, index Index) {
	c.Lock()
	defer c.Unlock()
	c.indexes[ctype] = append(c.indexes[ctype], index)
}
