package fdbx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
)

func newV610cursor(conn *v610Conn, id string, rtp RecordType, opts ...Option) (_ *v610cursor, err error) {
	var opt *options

	if opt, err = selectOpts(opts); err != nil {
		return
	}

	if id == "" {
		uid := uuid.New()
		id = fmt.Sprintf("%x", uid[:])
	}

	from := fdbKey(conn.db, rtp.ID, opt.from)
	return &v610cursor{
		id: id,

		From:    from,
		To:      fdbKey(conn.db, rtp.ID, opt.to),
		Pos:     from,
		Page:    opt.page,
		Limit:   opt.limit,
		Index:   rtp.ID,
		IsEmpty: false,

		conn:   conn,
		rtp:    &rtp,
		filter: opt.filter,
	}, nil
}

type v610cursor struct {
	id string

	From    fdb.Key `json:"from"`
	To      fdb.Key `json:"to"`
	Pos     fdb.Key `json:"pos"`
	Page    int     `json:"page"`
	Limit   int     `json:"limit"`
	Index   uint16  `json:"index"`
	IsEmpty bool    `json:"empty"`

	rtp    *RecordType
	conn   *v610Conn
	filter Predicat
}

// ********************** As Record **********************

func (cur *v610cursor) FdbxID() string { return cur.id }
func (cur *v610cursor) FdbxType() RecordType {
	return RecordType{
		ID:  CursorTypeID,
		New: func(id string) (Record, error) { return &v610cursor{id: id}, nil },
	}
}
func (cur *v610cursor) FdbxIndex(Indexer) error        { return nil }
func (cur *v610cursor) FdbxMarshal() ([]byte, error)   { return json.Marshal(cur) }
func (cur *v610cursor) FdbxUnmarshal(buf []byte) error { return json.Unmarshal(buf, cur) }

// ********************** Public **********************

func (cur *v610cursor) Empty() bool  { return cur.IsEmpty }
func (cur *v610cursor) Close() error { cur.IsEmpty = true; return cur.drop() }

func (cur *v610cursor) Next(db DB, skip uint8) ([]Record, error) {
	return cur.getPage(db, skip, false)
}

func (cur *v610cursor) Prev(db DB, skip uint8) ([]Record, error) {
	return cur.getPage(db, skip, true)
}

func (cur *v610cursor) Select(ctx context.Context) (<-chan Record, <-chan error) {
	recs := make(chan Record)
	errs := make(chan error, 1)
	go cur.readAll(ctx, recs, errs)
	return recs, errs
}

// ********************** Private **********************

func (cur *v610cursor) applyOpts(opts []Option) (err error) {
	opt := new(options)

	for i := range opts {
		if err = opts[i](opt); err != nil {
			return
		}
	}

	if opt.from != nil {
		cur.From = opt.from
	}

	if opt.to != nil {
		cur.To = append(opt.to, tail...)
	}

	if opt.page > 0 {
		cur.Page = opt.page
	}

	if opt.limit > 0 {
		cur.Limit = opt.limit
	}

	cur.IsEmpty = false
	cur.rtp.ID = cur.Index
	cur.filter = opt.filter
	return nil
}

func (cur *v610cursor) drop() error {
	return cur.conn.Tx(func(db DB) error { return db.Drop(nil, cur) })
}

func (cur *v610cursor) getPage(db DB, skip uint8, reverse bool) (list []Record, err error) {
	var ok bool
	var rng fdb.KeyRange
	var db610 *v610db

	defer func() {
		if err == nil {
			err = db.Save(nil, cur)
		}
	}()

	if db610, ok = db.(*v610db); !ok {
		return nil, ErrIncompatibleDB.WithStack()
	}

	opt := fdb.RangeOptions{Limit: cur.Page, Mode: fdb.StreamingModeWantAll, Reverse: reverse}

	if reverse {
		rng.Begin = cur.From
		rng.End = cur.Pos
	} else {
		rng.Begin = cur.Pos
		rng.End = cur.To
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

		cur.Pos = append(rows[rlen-1].Key, tail...)
		opt.Limit = cur.Page

		if reverse {
			rng.End = cur.Pos
		} else {
			rng.Begin = cur.Pos
		}
	}

	var lastKey fdb.Key

	if list, lastKey, err = getRange(db610.conn.db, db610.tx, rng, opt, cur.rtp, cur.filter); err != nil {
		return
	}

	if reverse {
		reverseList(list)
	}

	if len(lastKey) > 0 {
		cur.Pos = append(lastKey, tail...)
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

func (cur *v610cursor) readAll(ctx context.Context, recs chan Record, errs chan error) {
	var err error
	var list []Record

	defer close(recs)
	defer close(errs)

	count := 0
	for !cur.IsEmpty && ctx.Err() == nil && (cur.Limit == 0 || count < cur.Limit) {
		if err = cur.conn.Tx(func(db DB) (exp error) {
			list, exp = cur.getPage(db, 0, false)
			return
		}); err != nil {
			errs <- err
			return
		}

		for i := range list {
			select {
			case recs <- list[i]:
				count++
				if cur.Limit > 0 && count >= cur.Limit {
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
