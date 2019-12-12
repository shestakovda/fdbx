package fdbx

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// PunchSize - размер ожидания в случае отсутствия задач
var PunchSize = time.Minute

func newV610queue(conn *v610Conn, rtp RecordType, prefix []byte) (*v610queue, error) {
	return &v610queue{
		cn:  conn,
		rtp: rtp,
		pf:  prefix,
		wk:  conn.key(rtp.ID, prefix, []byte{0xFF, 0xFF}),
		kr: fdb.KeyRange{
			Begin: conn.key(rtp.ID, prefix, []byte{0x00}),
			End:   conn.key(rtp.ID, prefix, []byte{0xFF}),
		},
	}, nil
}

type v610queue struct {
	pf  []byte
	rtp RecordType
	cn  *v610Conn
	wk  fdb.Key
	kr  fdb.KeyRange
}

func (q *v610queue) Ack(db DB, id []byte) error {
	var ok bool
	var db610 *v610db

	if db == nil {
		return ErrNullDB.WithStack()
	}

	if db610, ok = db.(*v610db); !ok {
		return ErrIncompatibleDB.WithStack()
	}

	db610.tx.Clear(q.cn.key(q.rtp.ID, q.pf, []byte{0xFF}, id, []byte{byte(len(id))}))
	return nil
}

func (q *v610queue) Pub(db DB, id []byte, t time.Time) (err error) {
	var ok bool
	var db610 *v610db

	if db == nil {
		return ErrNullDB.WithStack()
	}

	if db610, ok = db.(*v610db); !ok {
		return ErrIncompatibleDB.WithStack()
	}

	if t.IsZero() {
		t = time.Now()
	}

	when := make([]byte, 8)
	binary.BigEndian.PutUint64(when, uint64(t.UnixNano()))

	// set task
	db610.tx.Set(q.cn.key(q.rtp.ID, q.pf, when, id, []byte{byte(len(id))}), nil)

	// update watch
	db610.tx.Set(q.wk, when)
	return nil
}

func (q *v610queue) Sub(ctx context.Context) (<-chan Record, <-chan error) {
	modc := make(chan Record)
	errc := make(chan error, 1)

	go func() {
		var m Record
		var err error

		defer close(errc)
		defer close(modc)
		defer func() {
			if rec := recover(); rec != nil {

				if err, ok := rec.(error); ok {
					errc <- ErrQueuePanic.WithReason(err)
				} else {
					errc <- ErrQueuePanic.WithReason(fmt.Errorf("%+v", rec))
				}
			}
		}()

		for {

			if m, err = q.SubOne(ctx); err != nil {
				errc <- err
				return
			}

			if m == nil {
				continue
			}

			select {
			case modc <- m:
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			}
		}
	}()

	return modc, errc
}

func (q *v610queue) SubOne(ctx context.Context) (_ Record, err error) {
	var list []Record

	if list, err = q.SubList(ctx, 1); err != nil {
		return
	}

	if len(list) > 0 {
		return list[0], nil
	}

	return nil, ErrRecordNotFound.WithStack()
}

func (q *v610queue) nextTaskDistance() (d time.Duration, err error) {
	d = PunchSize
	_, err = q.cn.fdb.ReadTransact(func(tx fdb.ReadTransaction) (_ interface{}, e error) {
		rows := tx.GetRange(q.kr, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll, Limit: 1}).GetSliceOrPanic()
		if len(rows) > 0 {
			pflen := 4 + len(q.pf)
			iwhen := int64(binary.BigEndian.Uint64(rows[0].Key[pflen : pflen+8]))
			if wait := time.Unix(0, iwhen).Sub(time.Now()); wait > 0 {
				d = wait + time.Millisecond
			}
		}
		return nil, nil
	})
	return d, err
}

func (q *v610queue) SubList(ctx context.Context, limit uint) (list []Record, err error) {
	var ids [][]byte
	var wait fdb.FutureNil
	var punch time.Duration

	for len(list) == 0 {

		if wait != nil {
			if punch, err = q.nextTaskDistance(); err != nil {
				return
			}

			wc := make(chan struct{}, 1)
			go func() { defer close(wc); wait.BlockUntilReady(); wc <- struct{}{} }()

			func() {
				wctx, cancel := context.WithTimeout(ctx, punch)
				defer cancel()

				select {
				case <-wc:
				case <-wctx.Done():
					wait.Cancel()
					return
				}
			}()
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// select ids in self tx
		_, err = q.cn.fdb.Transact(func(tx fdb.Transaction) (_ interface{}, e error) {
			var rows []fdb.KeyValue

			now := make([]byte, 8)
			ids = make([][]byte, 0, limit)
			binary.BigEndian.PutUint64(now, uint64(time.Now().UnixNano()))

			kr := fdb.KeyRange{Begin: q.cn.key(q.rtp.ID, q.pf, []byte{0x00}), End: q.cn.key(q.rtp.ID, q.pf, now)}

			lim := int(limit) - len(list)

			if lim < 1 {
				return nil, nil
			}

			// must lock this range from parallel reads
			if e = tx.AddWriteConflictRange(kr); e != nil {
				return
			}

			opts := fdb.RangeOptions{Mode: fdb.StreamingModeWantAll, Limit: lim}

			if rows = tx.GetRange(kr, opts).GetSliceOrPanic(); len(rows) == 0 {
				wait = tx.Watch(q.wk)
				return nil, nil
			}

			for i := range rows {
				klen := len(rows[i].Key)
				rln := rows[i].Key[klen-1 : klen]
				rid := rows[i].Key[klen-int(rln[0])-1 : klen-1]
				ids = append(ids, rid)

				// move to lost
				tx.Set(q.cn.key(q.rtp.ID, q.pf, []byte{0xFF}, rid, rln), nil)
				tx.Clear(rows[i].Key)
			}

			return nil, nil
		})
		if err != nil {
			return
		}

		if len(ids) == 0 {
			continue
		}

		recs := make([]Record, len(ids))
		for i := range ids {
			if recs[i], err = q.rtp.New(ids[i]); err != nil {
				return
			}
		}

		if err = q.cn.Tx(func(db DB) error { return db.Load(recs...) }); err != nil {
			return
		}

		list = append(list, recs...)
	}

	return list, nil
}

func (q *v610queue) GetLost(limit uint) ([]Record, error) { return nil, nil }
