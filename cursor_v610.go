package fdbx

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
)

func newV610cursor(conn *v610Conn, id string, rtp RecordType, opts ...Option) (_ *v610cursor, err error) {
	var opt *options
	var start fdb.Key

	if opt, err = selectOpts(opts); err != nil {
		return
	}

	if id == "" {
		uid := uuid.New()
		id = fmt.Sprintf("%x", uid[:])
	}

	from := fdbKey(conn.db, rtp.ID, opt.from)
	to := fdbKey(conn.db, rtp.ID, opt.to)

	if opt.reverse != nil {
		start = to
	} else {
		start = from
	}

	return &v610cursor{
		id: id,

		From:    from,
		To:      to,
		Pos:     start,
		Page:    opt.page,
		Limit:   opt.limit,
		Index:   rtp.ID,
		IsEmpty: false,
		Reverse: opt.reverse != nil,
		Created: time.Now(),

		conn: conn,
		rtp:  &rtp,
		cond: opt.cond,
	}, nil
}

type v610cursor struct {
	id string

	From    fdb.Key   `json:"from"`
	To      fdb.Key   `json:"to"`
	Pos     fdb.Key   `json:"pos"`
	Page    int       `json:"page"`
	Limit   int       `json:"limit"`
	Index   uint16    `json:"index"`
	IsEmpty bool      `json:"empty"`
	Reverse bool      `json:"rev"`
	Created time.Time `json:"created"`

	rtp  *RecordType
	conn *v610Conn
	cond Condition
}

// ********************** As Record **********************

func (cur *v610cursor) FdbxID() string { return cur.id }
func (cur *v610cursor) FdbxType() RecordType {
	return RecordType{
		ID:  CursorTypeID,
		New: func(ver uint8, id string) (Record, error) { return &v610cursor{id: id}, nil },
	}
}
func (cur *v610cursor) FdbxIndex(idx Indexer) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(cur.Created.UTC().UnixNano()))
	idx.Index(CursorIndexID, buf[:])
	return nil
}
func (cur *v610cursor) FdbxMarshal() ([]byte, error)   { return json.Marshal(cur) }
func (cur *v610cursor) FdbxUnmarshal(buf []byte) error { return json.Unmarshal(buf, cur) }

// ********************** Public **********************

func (cur *v610cursor) Empty() bool  { return cur.IsEmpty }
func (cur *v610cursor) Close() error { cur.IsEmpty = true; return cur.drop() }

func (cur *v610cursor) Next(db DB, skip uint8) ([]Record, error) {
	return cur.getPage(db, skip, true)
}

func (cur *v610cursor) Prev(db DB, skip uint8) ([]Record, error) {
	return cur.getPage(db, skip, false)
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

	if opt.reverse != nil {
		cur.Reverse = true
	}

	cur.IsEmpty = false
	cur.rtp.ID = cur.Index
	cur.cond = opt.cond
	return nil
}

func (cur *v610cursor) drop() error {
	return cur.conn.Tx(func(db DB) error { return db.Drop(nil, cur) })
}

func (cur *v610cursor) getPage(db DB, skip uint8, next bool) (list []Record, err error) {
	var ok bool
	var rev bool
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

	if next {
		rev = cur.Reverse
	} else {
		rev = !cur.Reverse
	}

	opt := fdb.RangeOptions{Limit: cur.Page, Mode: fdb.StreamingModeSerial, Reverse: rev}

	if rev {
		rng.Begin = cur.From
		rng.End = cur.Pos
	} else {
		rng.Begin = cur.Pos
		rng.End = cur.To
	}

	if skip > 0 {
		opt.Limit = int(skip) * cur.Page
		if rev {
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

		if rev {
			rng.End = cur.Pos
		} else {
			rng.Begin = cur.Pos
		}
	}

	if list, cur.Pos, err = getRange(db610.conn.db, db610.tx, rng, opt, cur.rtp, cur.cond, !next); err != nil {
		return
	}

	if !next {
		ReverseList(list)
	}

	cur.IsEmpty = len(list) < cur.Page
	return list, nil
}

func ReverseList(list []Record) {
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
			list, exp = cur.getPage(db, 0, true)
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
