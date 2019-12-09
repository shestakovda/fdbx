package fdbx

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newV610Conn(db uint16) (conn *v610Conn, err error) {
	conn = &v610Conn{baseConn: newBaseConn(db)}

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
	*baseConn

	fdb fdb.Database
}

// ********************** Public **********************

func (c *v610Conn) ClearDB() error {
	_, exp := c.fdb.Transact(func(tx fdb.Transaction) (_ interface{}, err error) {
		tx.ClearRange(fdb.KeyRange{Begin: c.key(0), End: c.key(0xFFFF, []byte{0xFF})})
		return
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

func (c *v610Conn) Queue(qtype uint16, f Fabric) (Queue, error) { return newV610queue(c, qtype, f) }
