package fdbx

import (
	"context"
	"encoding/json"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
)

func v610CursorFabric(conn *v610Conn, id []byte, rtp RecordType) (*v610cursor, error) {
	return &v610cursor{
		id:   id,
		rtp:  rtp,
		conn: conn,
		From: nil,
		To:   []byte{0xFF},
	}, nil
}

func newV610cursor(conn *v610Conn, rtp RecordType, start []byte, pageSize uint) (*v610cursor, error) {
	uid := uuid.New()

	if len(start) == 0 {
		start = []byte{0x00}
	}

	if pageSize <= 0 {
		pageSize = 100
	}

	return &v610cursor{
		id:   uid[:],
		Pos:  conn.key(rtp.ID, start),
		Page: int(pageSize),
		conn: conn,
		rtp:  rtp,
		From: nil,
		To:   []byte{0xFF},
	}, nil
}

type v610cursor struct {
	id []byte

	From    []byte  `json:"from"`
	To      []byte  `json:"to"`
	Pos     fdb.Key `json:"pos"`
	Page    int     `json:"page"`
	IsEmpty bool    `json:"empty"`

	rtp  RecordType
	conn *v610Conn
}

// ********************** As Record **********************

func (cur *v610cursor) FdbxID() []byte { return cur.id[:] }
func (cur *v610cursor) FdbxType() RecordType {
	return RecordType{
		ID:  CursorTypeID,
		New: func(id []byte) (Record, error) { return &v610cursor{id: id}, nil },
	}
}
func (cur *v610cursor) FdbxIndex(Indexer) error        { return nil }
func (cur *v610cursor) FdbxMarshal() ([]byte, error)   { return json.Marshal(cur) }
func (cur *v610cursor) FdbxUnmarshal(buf []byte) error { return json.Unmarshal(buf, cur) }

// ********************** Public **********************

func (cur *v610cursor) Empty() bool  { return cur.IsEmpty }
func (cur *v610cursor) Close() error { cur.IsEmpty = true; return cur.drop() }

func (cur *v610cursor) Next(db DB, skip uint8) ([]Record, error) {
	return cur.getPage(db, skip, false, nil)
}

func (cur *v610cursor) Prev(db DB, skip uint8) ([]Record, error) {
	return cur.getPage(db, skip, true, nil)
}

func (cur *v610cursor) Select(ctx context.Context, opts ...Option) (<-chan Record, <-chan error) {
	recs := make(chan Record)
	errs := make(chan error, 1)
	go cur.readAll(ctx, recs, errs, opts...)
	return recs, errs
}

// ********************** Private **********************

func (cur *v610cursor) drop() error { return cur.conn.Tx(func(db DB) error { return db.Drop(cur) }) }

func (cur *v610cursor) getPage(db DB, skip uint8, reverse bool, filter Predicat) (list []Record, err error) {
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

	opt := fdb.RangeOptions{Limit: cur.Page, Mode: fdb.StreamingModeWantAll, Reverse: reverse}

	if reverse {
		rng.Begin = cur.conn.key(cur.rtp.ID, cur.From)
		rng.End = cur.Pos
	} else {
		rng.Begin = cur.Pos
		rng.End = cur.conn.key(cur.rtp.ID, cur.To)
	}

	if skip > 0 {
		opt.Limit = int(skip) * cur.Page
		if reverse {
			opt.Limit++
		}

		rows := db610.tx.GetRange(rng, opt).GetSliceOrPanic()
		rlen := len(rows)

		if rlen < opt.Limit {
			cur.IsEmpty = true
			return nil, nil
		}

		cur.Pos = append(rows[rlen-1].Key, 0xFF)
		opt.Limit = cur.Page

		if reverse {
			rng.End = cur.Pos
		} else {
			rng.Begin = cur.Pos
		}
	}

	var lastKey fdb.Key

	if list, lastKey, err = db610.getRange(rng, opt, cur.rtp, filter); err != nil {
		return
	}

	if reverse {
		reverseList(list)
	}

	if len(lastKey) > 0 {
		cur.Pos = append(lastKey, 0xFF)
	}

	cur.IsEmpty = len(list) < cur.Page
	return list, nil
}

func reverseList(list []Record) {
	llen := len(list)

	if llen == 0 {
		return
	}

	for i := llen/2 - 1; i >= 0; i-- {
		opp := llen - 1 - i
		list[i], list[opp] = list[opp], list[i]
	}
}

func (cur *v610cursor) readAll(ctx context.Context, recs chan Record, errs chan error, opts ...Option) {
	var err error
	var list []Record

	defer close(recs)
	defer close(errs)

	o := new(options)

	for i := range opts {
		if err = opts[i](o); err != nil {
			errs <- err
			return
		}
	}

	cur.From = nil
	if len(o.from) > 0 {
		cur.From = o.from
	}

	cur.To = []byte{0xFF}
	if len(o.to) > 0 {
		cur.To = o.to
	}

	count := 0
	for !cur.IsEmpty && ctx.Err() == nil && (o.limit == 0 || count < o.limit) {
		if err = cur.conn.Tx(func(db DB) (exp error) {
			list, exp = cur.getPage(db, 0, false, o.filter)
			return
		}); err != nil {
			errs <- err
			return
		}

		for i := range list {
			select {
			case recs <- list[i]:
				count++
				if o.limit > 0 && count >= o.limit {
					cur.IsEmpty = true
					return
				}
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
		}
	}
}
