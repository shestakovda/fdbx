package fdbx

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newV610db(c Conn, tx fdb.Transaction) (db *v610db, err error) {
	db = &v610db{conn: c, tx: tx}

	return db, nil
}

type v610db struct {
	conn Conn
	tx   fdb.Transaction
}

func (db *v610db) Get(ctype uint16, id []byte) (_ []byte, err error) {
	var key []byte

	if key, err = db.conn.Key(ctype, id); err != nil {
		return
	}

	return db.tx.Get(fdb.Key(key)).Get()
}

func (db *v610db) Set(ctype uint16, id, value []byte) (err error) {
	var key []byte

	if key, err = db.conn.Key(ctype, id); err != nil {
		return
	}

	db.tx.Set(fdb.Key(key), value)
	return nil
}

func (db *v610db) Del(ctype uint16, id []byte) (err error) {
	var key []byte

	if key, err = db.conn.Key(ctype, id); err != nil {
		return
	}

	db.tx.Clear(fdb.Key(key))
	return nil
}
