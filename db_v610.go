package fdbx

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
)

// ChunkType is collection number for storing blob chunks. Default uint16 max value
var ChunkType = uint16(65535)

// ChunkSize is max chunk length. Default 100 Kb - fdb value limit
var ChunkSize = 100000

// MaxChunkSize is max possible chunk size.
const MaxChunkSize = 100000

// GZipSize is a value len more then GZipSize cause gzip processing
var GZipSize = 860

const (
	flagGZip  = uint8(1 << 6)
	flagChunk = uint8(1 << 7)
)

func newV610db(c *v610Conn, tx fdb.Transaction) (db *v610db, err error) {
	db = &v610db{conn: c, tx: tx}

	return db, nil
}

type v610db struct {
	conn *v610Conn
	tx   fdb.Transaction
}

func (db *v610db) Clear() error {
	dbtype := db.conn.DB()

	// all plain data
	begin := make(fdb.Key, 2)
	binary.BigEndian.PutUint16(begin[0:2], dbtype)
	end := make(fdb.Key, 5)
	binary.BigEndian.PutUint16(end[0:2], dbtype)
	binary.BigEndian.PutUint16(end[2:4], 0xFFFF)
	end[4] = 0xFF
	db.tx.ClearRange(fdb.KeyRange{Begin: begin, End: end})

	return nil
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

func (db *v610db) Save(models ...Model) (err error) {
	for i := range models {
		if err = db.save(models[i]); err != nil {
			return
		}
	}

	return nil
}

func (db *v610db) Load(models ...Model) (err error) {
	var key []byte

	// query all futures to leverage wait time
	futures := make([]fdb.FutureByteSlice, 0, len(models))

	for i := range models {
		if key, err = db.conn.MKey(models[i]); err != nil {
			return
		}

		futures = append(futures, db.tx.Get(fdb.Key(key)))
	}

	for i := range futures {
		if err = db.load(models[i], futures[i]); err != nil {
			return
		}
	}

	return nil
}

func (db *v610db) save(m Model) (err error) {
	var flags uint8
	var value, key, idx []byte

	// basic model key
	if key, err = db.conn.MKey(m); err != nil {
		return
	}

	// type index list
	indexes := db.conn.Indexes(m.Type())

	// old data dump for index invalidate
	if dump := m.Dump(); len(dump) > 0 {
		for _, index := range indexes {
			if idx, err = index(dump); err != nil {
				return
			}

			db.tx.Clear(fdb.Key(idx))
		}
	}

	// plain object buffer
	if value, err = m.Pack(); err != nil {
		return
	}

	// new index keys
	for _, index := range indexes {
		if idx, err = index(value); err != nil {
			return
		}

		db.tx.Set(fdb.Key(idx), nil)
	}

	// so long, try to reduce
	if len(value) > GZipSize {
		if value, err = db.gzipValue(&flags, value); err != nil {
			return
		}
	}

	// sooooooo long, we must split and save as blob
	if len(value) > ChunkSize {
		if value, err = db.saveBlob(&flags, value); err != nil {
			return
		}
	}

	db.tx.Set(fdb.Key(key), append([]byte{flags}, value...))
	return nil
}

func (db *v610db) load(m Model, fb fdb.FutureByteSlice) (err error) {
	var value []byte

	if value, err = fb.Get(); err != nil {
		return
	}

	if len(value) == 0 {
		// it's model responsibility for loading control
		return nil
	}

	flags := value[0]
	value = value[1:]

	// blob data
	if flags&flagChunk > 0 {
		if value, err = db.loadBlob(value); err != nil {
			return
		}
	}

	if len(value) == 0 {
		// it's model responsibility for loading control
		return nil
	}

	// gzip data
	if flags&flagGZip > 0 {
		if value, err = db.gunzipValue(value); err != nil {
			return
		}
	}

	// plain buffer
	if err = m.Load(value); err != nil {
		return
	}

	return nil
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
	var part, key []byte
	var index [2]byte

	*flags |= flagChunk
	blobID := uuid.New()

	if key, err = db.conn.Key(ChunkType, blobID[:]); err != nil {
		return
	}

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
		db.tx.Set(fdb.Key(append(key, index[0], index[1])), part)
		i++
	}

	return blobID[:], nil
}

func (db *v610db) loadBlob(value []byte) (blob []byte, err error) {
	var key []byte
	var kv fdb.KeyValue

	if key, err = db.conn.Key(ChunkType, value); err != nil {
		return
	}

	kr := fdb.KeyRange{Begin: fdb.Key(append(key, 0)), End: fdb.Key(append(key, 255))}
	res := db.tx.GetRange(kr, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

	for res.Advance() {
		if kv, err = res.Get(); err != nil {
			return
		}
		blob = append(blob, kv.Value...)
	}
	return blob, nil
}
