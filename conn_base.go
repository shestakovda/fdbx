package fdbx

import (
	"encoding/binary"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newBaseConn(db uint16) *baseConn {
	return &baseConn{
		db:      db,
		indexes: make(map[uint16]IndexFunc, 8),
	}
}

type baseConn struct {
	sync.RWMutex
	db      uint16
	indexes map[uint16]IndexFunc
}

// ********************** Public **********************

func (c *baseConn) RegisterIndex(recordTypeID uint16, idxFunc IndexFunc) {
	c.indexes[recordTypeID] = idxFunc
}

// ********************** Private **********************

func (c *baseConn) key(typeID uint16, parts ...[]byte) fdb.Key {
	mem := 4

	for i := range parts {
		mem += len(parts)
	}

	key := make(fdb.Key, 4, mem)

	binary.BigEndian.PutUint16(key[0:2], c.db)
	binary.BigEndian.PutUint16(key[2:4], typeID)

	for i := range parts {
		key = append(key, parts[i]...)
	}

	return key
}
