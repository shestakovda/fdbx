package fdbx

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/golang/glog"

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

func (c *v610Conn) At(id uint16) Conn {
	return &v610Conn{db: id, fdb: c.fdb}
}

func (c *v610Conn) Tx(h TxHandler) error {
	_, exp := c.fdb.Transact(func(tx fdb.Transaction) (interface{}, error) {
		return nil, h(&v610db{conn: c, tx: tx})
	})

	return exp
}

func (c *v610Conn) Queue(rtp RecordType, prefix string, opts ...Option) (Queue, error) {
	opt := new(options)

	for i := range opts {
		if err := opts[i](opt); err != nil {
			return nil, err
		}
	}

	return &v610queue{
		cn:  c,
		rtp: &rtp,
		pf:  prefix,
		nf:  opt.onNotFound,
	}, nil
}

func (c *v610Conn) Cursor(rtp RecordType, opts ...Option) (Cursor, error) {
	return newV610cursor(c, "", rtp, opts...)
}

func (c *v610Conn) CursorID(rtp RecordType, opts ...Option) (CursorID, error) {
	return newidscursor(c, "", rtp.ID, opts...)
}

func (c *v610Conn) Waiter(typeID uint16) Waiter {
	return newV610Waiter(c, typeID)
}

func (c *v610Conn) FileSystem(typeID uint16) FileSystem {
	return newV610FileSystem(c, typeID)
}

func (c *v610Conn) LoadCursor(id string, rf RecordFabric, opts ...Option) (_ Cursor, err error) {
	var cur *v610cursor

	if cur, err = newV610cursor(c, id, RecordType{New: rf}); err != nil {
		return
	}

	if err = c.Tx(func(db DB) error { return db.Load(nil, cur) }); err != nil {
		return
	}

	return cur, cur.ApplyOpts(opts...)
}

func (c *v610Conn) StartClearDaemon() {
	defer func() {
		if rec := recover(); rec != nil {
			glog.Errorf("fdbx: cursor clear daemon panic: %+v", rec)
			go c.StartClearDaemon()
		}
	}()

	for {
		err := c.Tx(func(db DB) (exp error) {
			var buf [8]byte
			var ids []string

			before := time.Now().Add(-24 * time.Hour)

			binary.BigEndian.PutUint64(buf[:], uint64(before.UTC().UnixNano()))

			opts := []Option{
				Limit(10000),
				To(buf[:]),
			}

			if ids, exp = db.SelectIDs(CursorIndexID, opts...); exp != nil {
				return
			}

			if len(ids) == 0 {
				return
			}

			list := make([]Record, len(ids))

			for i := range ids {
				list[i] = &v610cursor{id: ids[i]}
			}

			if exp = db.Drop(nil, list...); exp != nil {
				return
			}

			glog.Infof("fdbx: cursor clear: drop %d items", len(list))
			return nil
		})

		if err != nil {
			glog.Errorf("fdbx: cursor clear daemon error: %+v", err)
			return
		}

		time.Sleep(time.Hour)
	}
}

// ********************** Private **********************

func fdbKeyBuf(buf *bytes.Buffer, dbID, typeID uint16, parts ...[]byte) fdb.Key {
	mem := 4
	plen := 0

	for i := range parts {
		mem += len(parts[i])
	}

	buf.Grow(mem)

	var idx [4]byte
	binary.BigEndian.PutUint16(idx[0:2], dbID)
	binary.BigEndian.PutUint16(idx[2:4], typeID)

	buf.Write(idx[:])

	for i := range parts {
		if plen = len(parts[i]); plen > 0 {
			buf.Write(parts[i])
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
	rid := S2B(rec.FdbxID())
	rln := byte(len(rid))
	key := make(fdb.Key, rln+5)

	binary.BigEndian.PutUint16(key[0:2], dbID)
	binary.BigEndian.PutUint16(key[2:4], rec.FdbxType().ID)

	copy(key[4:], rid)
	key[4+rln] = rln
	return key
}

func recTypeKey(dbID uint16, typeID uint16, recID string) fdb.Key {
	rid := S2B(recID)
	rln := byte(len(rid))
	key := make(fdb.Key, rln+5)

	binary.BigEndian.PutUint16(key[0:2], dbID)
	binary.BigEndian.PutUint16(key[2:4], typeID)

	copy(key[4:], rid)
	key[4+rln] = rln
	return key
}
