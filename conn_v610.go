package fdbx

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var tail = bytes.Repeat([]byte{0xFF}, 17)

func newV610Conn(db uint16) (conn *v610Conn, err error) {
	conn = &v610Conn{db: db}

	// Installed version check
	if err = fdb.APIVersion(int(ConnVersion610)); err != nil {
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

// ********************** Public **********************

func (c *v610Conn) ClearDB() error {
	_, exp := c.fdb.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.ClearRange(fdb.KeyRange{
			Begin: fdbKey(c.db, 0x0000),
			End:   fdbKey(c.db, 0xFFFF, []byte{0xFF}),
		})
		return nil, nil
	})
	return exp
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

func (c *v610Conn) Queue(rtp RecordType, prefix string) (Queue, error) {
	return newV610queue(c, rtp, prefix)
}

func (c *v610Conn) Cursor(rtp RecordType, opts ...Option) (Cursor, error) {
	return newV610cursor(c, "", rtp, opts...)
}

func (c *v610Conn) LoadCursor(id string, rf RecordFabric, opts ...Option) (_ Cursor, err error) {
	var cur *v610cursor

	if cur, err = newV610cursor(c, id, RecordType{New: rf}); err != nil {
		return
	}

	if err = c.Tx(func(db DB) error { return db.Load(nil, cur) }); err != nil {
		return
	}

	return cur, cur.applyOpts(opts)
}

// ********************** Private **********************

func fdbKeyBuf(buf *bytes.Buffer, dbID, typeID uint16, parts ...[]byte) fdb.Key {
	mem := 4
	// ptr := 4
	plen := 0

	for i := range parts {
		mem += len(parts[i])
	}

	buf.Grow(mem)

	// key := make(fdb.Key, mem)

	var idx [4]byte
	binary.BigEndian.PutUint16(idx[0:2], dbID)
	binary.BigEndian.PutUint16(idx[2:4], typeID)

	buf.Write(idx[:])

	for i := range parts {
		if plen = len(parts[i]); plen > 0 {
			buf.Write(parts[i])

			// copy(key[ptr:], parts[i])
			// ptr += plen
		}
	}

	return fdb.Key(buf.Bytes())
}

func fdbKey(dbID, typeID uint16, parts ...[]byte) fdb.Key {
	mem := 4
	ptr := 4
	plen := 0

	for i := range parts {
		mem += len(parts[i])
	}

	key := make(fdb.Key, mem)

	binary.BigEndian.PutUint16(key[0:2], dbID)
	binary.BigEndian.PutUint16(key[2:4], typeID)

	for i := range parts {
		if plen = len(parts[i]); plen > 0 {
			copy(key[ptr:], parts[i])
			ptr += plen
		}
	}

	return key
}

func recKey(dbID uint16, rec Record) fdb.Key {
	rid := s2b(rec.FdbxID())
	rln := byte(len(rid))
	key := make(fdb.Key, rln+5)

	binary.BigEndian.PutUint16(key[0:2], dbID)
	binary.BigEndian.PutUint16(key[2:4], rec.FdbxType().ID)

	copy(key[4:], rid)
	key[4+rln] = rln
	return key
}

func s2b(s string) []byte {
	if s == "" {
		return nil
	}
	return *(*[]byte)(unsafe.Pointer(&s))
}

func b2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return *(*string)(unsafe.Pointer(&b))
}
