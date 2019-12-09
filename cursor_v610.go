package fdbx

import (
	"context"

	"github.com/google/uuid"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newV610cursor(conn *v610Conn, typeID uint16, fab Fabric) (*v610cursor, error) {
	return &v610cursor{
		conn:   conn,
		fabric: fab,
		typeID: typeID,
	}, nil
}

type v610cursor struct {
	id uuid.UUID

	pos   fdb.Key
	page  int
	empty bool

	typeID uint16
	fabric Fabric

	conn *v610Conn
}

// ********************** As Record **********************

func (cur *v610cursor) FdbxID() []byte               { return cur.id[:] }
func (cur *v610cursor) FdbxType() uint16             { return CursorType }
func (cur *v610cursor) FdbxMarshal() ([]byte, error) { return nil, nil }
func (cur *v610cursor) FdbxUnmarshal([]byte) error   { return nil }

// ********************** Public **********************

func (cur *v610cursor) Empty() bool                              { return cur.empty }
func (cur *v610cursor) Close() error                             { cur.empty = true; return cur.drop() }
func (cur *v610cursor) Settings() (uint16, Fabric)               { return cur.typeID, cur.fabric }
func (cur *v610cursor) Next(db DB, skip uint8) ([]Record, error) { return cur.getPage(db, skip, false) }
func (cur *v610cursor) Prev(db DB, skip uint8) ([]Record, error) { return cur.getPage(db, skip, true) }

func (cur *v610cursor) Select(ctx context.Context) (<-chan Record, <-chan error) {
	recs := make(chan Record)
	errs := make(chan error, 1)
	go cur.readAll(ctx, recs, errs)
	return recs, errs
}

// ********************** Private **********************

func (cur *v610cursor) drop() error { return cur.conn.Tx(func(db DB) error { return db.Drop(cur) }) }

func (cur *v610cursor) getPage(db DB, skip uint8, reverse bool) (list []Record, err error) {
	var ok bool
	var rng fdb.KeyRange
	var db610 *v610db

	defer func() {
		if err == nil {
			err = db.Save(cur)
		}
	}()

	if db610, ok = db.(*v610db); !ok {
		return nil, ErrIncompatibleDB.WithStack()
	}

	opt := fdb.RangeOptions{Limit: cur.page, Mode: fdb.StreamingModeWantAll, Reverse: reverse}

	if reverse {
		rng.Begin = cur.conn.key(cur.typeID)
		rng.End = cur.pos
	} else {
		rng.Begin = cur.pos
		rng.End = cur.conn.key(cur.typeID, []byte{0xFF})
	}

	if skip > 0 {
		opt.Limit = int(skip) * cur.page
		rows := db610.tx.GetRange(rng, opt).GetSliceOrPanic()
		rlen := len(rows)
		cur.pos = append(rows[rlen-1].Key, 0xFF)

		if reverse {
			rng.End = cur.pos
		} else {
			rng.Begin = cur.pos
		}

		if rlen < opt.Limit {
			cur.empty = true
			return nil, nil
		}

		opt.Limit = cur.page
	}

	if list, err = db610.getRange(rng, opt, cur.fabric, nil); err != nil {
		return
	}

	llen := len(list)

	if llen > 0 {
		cur.pos = cur.conn.key(cur.typeID, list[llen-1].FdbxID(), []byte{0xFF})
	}

	cur.empty = !reverse && llen < cur.page
	return list, nil
}

func (cur *v610cursor) readAll(ctx context.Context, recs chan Record, errs chan error) {
	var err error
	var list []Record

	defer close(recs)
	defer close(errs)

	for !cur.empty && ctx.Err() == nil {
		if err = cur.conn.Tx(func(db DB) (exp error) {
			list, exp = cur.Next(db, 0)
			return
		}); err != nil {
			errs <- err
			return
		}

		for i := range list {
			select {
			case recs <- list[i]:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
		}
	}
}
