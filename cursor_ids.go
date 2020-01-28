package fdbx

import (
	"context"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
)

func newidscursor(conn *v610Conn, id string, typeID uint16, opts ...Option) (_ *idscursor, err error) {
	var opt *options
	var start fdb.Key

	if opt, err = selectOpts(opts); err != nil {
		return
	}

	if id == "" {
		uid := uuid.New()
		id = fmt.Sprintf("%x", uid[:])
	}

	from := fdbKey(conn.db, typeID, opt.from)
	to := fdbKey(conn.db, typeID, opt.to)

	if opt.reverse != nil {
		start = to
	} else {
		start = from
	}

	return &idscursor{
		id: id,

		From:    from,
		To:      to,
		Pos:     start,
		Page:    opt.page,
		Limit:   opt.limit,
		Index:   typeID,
		IsEmpty: false,
		Reverse: opt.reverse != nil,
		Created: time.Now(),

		conn: conn,
	}, nil
}

type idscursor struct {
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

	conn *v610Conn
}

// ********************** Public **********************

func (cur *idscursor) Empty() bool  { return cur.IsEmpty }
func (cur *idscursor) Close() error { cur.IsEmpty = true; return nil }

func (cur *idscursor) Next(db DB, skip uint8) ([]string, error) {
	return cur.getPage(db, skip, true)
}

func (cur *idscursor) Prev(db DB, skip uint8) ([]string, error) {
	return cur.getPage(db, skip, false)
}

func (cur *idscursor) Select(ctx context.Context) (<-chan string, <-chan error) {
	recs := make(chan string)
	errs := make(chan error, 1)
	go cur.readAll(ctx, recs, errs)
	return recs, errs
}

// ********************** Private **********************

func (cur *idscursor) getPage(db DB, skip uint8, next bool) (list []string, err error) {
	var ok bool
	var rev bool
	var rng fdb.KeyRange
	var db610 *v610db

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

	if list, cur.Pos, err = getRangeIDs2(db610.tx, rng, opt, !next); err != nil {
		return
	}

	if !next {
		ReverseStrings(list)
	}

	cur.IsEmpty = len(list) < cur.Page
	return list, nil
}

func ReverseStrings(list []string) {
	llen := len(list)

	if llen == 0 {
		return
	}

	for i := llen/2 - 1; i >= 0; i-- {
		opp := llen - 1 - i
		list[i], list[opp] = list[opp], list[i]
	}
}

func (cur *idscursor) readAll(ctx context.Context, recs chan string, errs chan error) {
	var err error
	var list []string

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
