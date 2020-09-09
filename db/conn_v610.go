package db

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/fdbx"
)

func newConnV610(id byte, opts ...Option) (cn *v610Conn, err error) {
	const verID = 610
	const badID = "Invalid database ID: %X"

	if id == 0xFF {
		return nil, ErrConnect.WithDetail(badID, id)
	}

	if err = fdb.APIVersion(verID); err != nil {
		return nil, ErrConnect.WithReason(err)
	}

	cn = &v610Conn{
		id: id,
		sk: fdbx.Key{id},
	}
	cn.ek = append(cn.sk, tail...)

	for i := range opts {
		if err = opts[i](&cn.options); err != nil {
			return nil, ErrConnect.WithReason(err)
		}
	}

	if len(cn.ClusterFile) > 0 {
		if cn.db, err = fdb.OpenDatabase(cn.ClusterFile); err != nil {
			return nil, ErrConnect.WithReason(err)
		}
	} else {
		if cn.db, err = fdb.OpenDefault(); err != nil {
			return nil, ErrConnect.WithReason(err)
		}
	}

	return cn, nil
}

type v610Conn struct {
	options
	id byte
	sk fdbx.Key
	ek fdbx.Key
	db fdb.Database
}

func (cn v610Conn) DB() byte { return cn.id }

func (cn v610Conn) Read(hdl func(Reader) error) error {
	if _, err := cn.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		return nil, hdl(&v610Reader{v610Conn: cn, tx: tx})
	}); err != nil {
		return ErrRead.WithReason(err)
	}
	return nil
}

func (cn v610Conn) Write(hdl func(Writer) error) error {
	if _, err := cn.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		return nil, hdl(&v610Writer{v610Reader: v610Reader{v610Conn: cn, tx: tx}, tx: tx})
	}); err != nil {
		return ErrWrite.WithReason(err)
	}
	return nil
}

func (cn v610Conn) Clear() error {
	if _, err := cn.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		begin := fdb.Key{cn.DB()}
		rng := fdb.KeyRange{Begin: begin, End: append(begin, tail...)}
		tx.ClearRange(&rng)
		return nil, nil
	}); err != nil {
		return ErrClear.WithReason(err)
	}
	return nil
}
