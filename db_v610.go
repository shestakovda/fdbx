package fdbx

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
)

const (
	flagGZip  = uint8(1 << 6)
	flagChunk = uint8(1 << 7)
)

func newV610db(c *v610Conn, tx fdb.Transaction) (*v610db, error) {
	return &v610db{conn: c, tx: tx}, nil
}

type v610db struct {
	conn *v610Conn
	tx   fdb.Transaction
}

// ********************** Public **********************

func (db *v610db) Get(typeID uint16, id []byte) ([]byte, error) {
	return db.tx.Get(db.conn.key(typeID, id)).Get()
}

func (db *v610db) Set(typeID uint16, id, value []byte) error {
	db.tx.Set(db.conn.key(typeID, id), value)
	return nil
}

func (db *v610db) Del(typeID uint16, id []byte) error {
	db.tx.Clear(db.conn.key(typeID, id))
	return nil
}

func (db *v610db) Save(recs ...Record) (err error) {
	for i := range recs {
		if err = db.save(recs[i]); err != nil {
			return
		}
	}

	return nil
}

func (db *v610db) Load(recs ...Record) (err error) {
	// query all futures to leverage wait time
	futures := make([]fdb.FutureByteSlice, len(recs))
	for i := range recs {
		futures[i] = db.tx.Get(db.conn.rkey(recs[i]))
	}

	for i := range futures {
		if err = db.load(recs[i], futures[i]); err != nil {
			return
		}
	}

	return nil
}

func (db *v610db) Drop(recs ...Record) (err error) {
	keys := make(map[int]fdb.Key, len(recs))
	futures := make([]fdb.FutureByteSlice, len(recs))

	for i := range recs {
		keys[i] = db.conn.rkey(recs[i])
		futures[i] = db.tx.Get(keys[i])
	}

	for i := range futures {
		if err = db.drop(recs[i], futures[i]); err != nil {
			return
		}

		db.tx.Clear(keys[i])
	}

	return nil
}

func (db *v610db) Select(typeID uint16, fab Fabric, opts ...Option) (list []Record, err error) {
	o := new(options)

	for i := range opts {
		if err = opts[i](o); err != nil {
			return
		}
	}

	opt := fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}

	if o.limit > 0 {
		opt.Limit = o.limit
	}

	from := []byte{0x00}
	if o.from != nil {
		from = o.from
	}

	to := []byte{0xFF}
	if o.to != nil {
		to = o.to
	}

	rng := fdb.KeyRange{Begin: db.conn.key(typeID, from), End: db.conn.key(typeID, to)}

	if list, _, err = db.getRange(rng, opt, fab, o.filter); err != nil {
		return
	}

	return list, err
}

// *********** private ***********

func (db *v610db) pack(buffer []byte) (_ []byte, err error) {
	var flags uint8

	// so long, try to reduce
	if len(buffer) > GZipSize {
		if buffer, err = db.gzipValue(&flags, buffer); err != nil {
			return
		}
	}

	// sooooooo long, we must split and save as blob
	if len(buffer) > ChunkSize {
		if buffer, err = db.saveBlob(&flags, buffer); err != nil {
			return
		}
	}

	return append([]byte{flags}, buffer...), nil
}

func (db *v610db) unpack(value []byte) (blobID, buffer []byte, err error) {
	flags := value[0]
	buffer = value[1:]

	// blob data
	if flags&flagChunk > 0 {
		blobID = buffer

		if buffer, err = db.loadBlob(buffer); err != nil {
			return
		}
	}

	// gzip data
	if flags&flagGZip > 0 {
		if buffer, err = db.gunzipValue(buffer); err != nil {
			return
		}
	}

	return blobID, buffer, nil
}

func (db *v610db) setIndexes(rec Record, buf []byte, drop bool) (err error) {
	var fnc IndexFunc
	var idx *v610Indexer

	if fnc = db.conn.indexes[rec.FdbxType()]; fnc == nil {
		return nil
	}

	if idx, err = newV610Indexer(); err != nil {
		return
	}

	if err = fnc(idx, buf); err != nil {
		return
	}

	for i := range idx.list {
		rid := rec.FdbxID()
		rln := []byte{byte(len(rid))}
		key := db.conn.key(idx.list[i].typeID, idx.list[i].value, rid, rln)

		if drop {
			db.tx.Clear(key)
		} else {
			db.tx.Set(key, nil)
		}
	}

	return nil
}

func (db *v610db) drop(rec Record, fb fdb.FutureByteSlice) (err error) {
	var buf, blobID []byte

	if buf, err = fb.Get(); err != nil {
		return
	}

	if len(buf) == 0 {
		return nil
	}

	if blobID, buf, err = db.unpack(buf); err != nil {
		return
	}

	if blobID != nil {
		if err = db.dropBlob(blobID); err != nil {
			return
		}
	}

	return db.setIndexes(rec, buf, true)
}

func (db *v610db) save(rec Record) (err error) {
	var buf []byte

	if buf, err = db.tx.Get(db.conn.rkey(rec)).Get(); err != nil {
		return
	}

	if len(buf) > 0 {
		if _, buf, err = db.unpack(buf); err != nil {
			return
		}

		if err = db.setIndexes(rec, buf, true); err != nil {
			return
		}
	}

	if buf, err = rec.FdbxMarshal(); err != nil {
		return
	}

	if err = db.setIndexes(rec, buf, false); err != nil {
		return
	}

	if buf, err = db.pack(buf); err != nil {
		return
	}

	db.tx.Set(db.conn.rkey(rec), buf)
	return nil
}

func (db *v610db) load(m Record, fb fdb.FutureByteSlice) (err error) {
	var buf []byte

	if buf, err = fb.Get(); err != nil {
		return
	}

	if len(buf) == 0 {
		return ErrRecordNotFound.WithStack()
	}

	if _, buf, err = db.unpack(buf); err != nil {
		return
	}

	return m.FdbxUnmarshal(buf)
}

func (db *v610db) gzipValue(flags *uint8, value []byte) ([]byte, error) {
	*flags |= flagGZip

	// TODO: sync.Pool
	buf := new(bytes.Buffer)

	if err := db.gzip(buf, bytes.NewReader(value)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (db *v610db) gunzipValue(value []byte) ([]byte, error) {
	// TODO: sync.Pool
	buf := new(bytes.Buffer)

	if err := db.gunzip(buf, bytes.NewReader(value)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (db *v610db) gzip(w io.Writer, r io.Reader) (err error) {
	gw := gzip.NewWriter(w)

	defer func() {
		e := gw.Close()
		if err == nil {
			err = e
		}
	}()

	if _, err = io.Copy(gw, r); err != nil {
		return ErrMemFail.WithReason(err)
	}

	return nil
}

func (db *v610db) gunzip(w io.Writer, r io.Reader) (err error) {
	var gr *gzip.Reader

	if gr, err = gzip.NewReader(r); err != nil {
		return ErrInvalidGZ.WithReason(err)
	}

	defer func() {
		e := gr.Close()
		if err == nil {
			err = e
		}
	}()

	if _, err = io.Copy(w, gr); err != nil {
		return ErrMemFail.WithReason(err)
	}

	return nil
}

func (db *v610db) saveBlob(flags *uint8, blob []byte) (value []byte, err error) {
	var i uint16
	var last bool
	var part []byte
	var index [2]byte

	*flags |= flagChunk
	blobID := uuid.New()

	// TODO: only up to 10M (transaction size)
	// split into multiple goroutines for speed
	for !last {
		// check tail
		if len(blob) <= ChunkSize {
			last = true
			part = blob
		} else {
			part = blob[:ChunkSize]
			blob = blob[ChunkSize:]
		}

		// save part
		binary.BigEndian.PutUint16(index[:], i)
		db.tx.Set(db.conn.key(ChunkType, blobID[:], index[:]), part)
		i++
	}

	return blobID[:], nil
}

func (db *v610db) loadBlob(value []byte) (blob []byte, err error) {
	var kv fdb.KeyValue

	res := db.tx.GetRange(fdb.KeyRange{
		Begin: db.conn.key(ChunkType, value),
		End:   db.conn.key(ChunkType, value, []byte{0xFF}),
	}, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

	for res.Advance() {
		if kv, err = res.Get(); err != nil {
			return
		}
		blob = append(blob, kv.Value...)
	}
	return blob, nil
}

func (db *v610db) dropBlob(value []byte) error {
	db.tx.ClearRange(fdb.KeyRange{
		Begin: db.conn.key(ChunkType, value),
		End:   db.conn.key(ChunkType, value, []byte{0xFF}),
	})
	return nil
}

func (db *v610db) getRange(
	rng fdb.Range,
	opt fdb.RangeOptions,
	fab Fabric,
	chk Predicat,
) (list []Record, lastKey fdb.Key, err error) {
	var rec Record
	var buf []byte

	rows := db.tx.GetRange(rng, opt).GetSliceOrPanic()
	list = make([]Record, 0, len(rows))
	load := make([]Record, 0, len(rows))

	for i := range rows {
		if opt.Reverse {
			if i == 0 {
				lastKey = rows[i].Key
			}
		} else {
			if i == len(rows)-1 {
				lastKey = rows[i].Key
			}
		}

		klen := len(rows[i].Key)
		idlen := int(rows[i].Key[klen-1])
		rid := rows[i].Key[klen-idlen-1 : klen-1]

		if rec, err = fab(rid); err != nil {
			return
		}

		if len(rows[i].Value) == 0 {
			load = append(load, rec)
			continue
		}

		if _, buf, err = db.unpack(rows[i].Value); err != nil {
			return
		}

		if err = rec.FdbxUnmarshal(buf); err != nil {
			return
		}

		add := true
		if chk != nil {
			if add, err = chk(rec); err != nil {
				return
			}
		}

		if add {
			list = append(list, rec)
		}
	}

	if len(load) > 0 {
		if err = db.Load(load...); err != nil {
			return
		}

		for i := range load {
			add := true
			if chk != nil {
				if add, err = chk(load[i]); err != nil {
					return
				}
			}

			if add {
				list = append(list, load[i])
			}
		}
	}

	return list, lastKey, nil
}
