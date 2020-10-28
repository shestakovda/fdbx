package orm

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"

	fbs "github.com/google/flatbuffers/go"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/typex"
)

const (
	nsData  byte = 0
	nsBLOB  byte = 1
	nsIndex byte = 2
	nsQueue byte = 3
	nsWatch byte = 4
	nsQuery byte = 5
)

const (
	qFlag byte = 0
	qList byte = 1
	qWork byte = 2
	qStat byte = 3
)

var gzLimit uint32 = 840
var loLimit uint32 = 100000

var zipPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}
var fbsPool = sync.Pool{New: func() interface{} { return fbs.NewBuilder(128) }}

var qTriggerKey = fdbx.Key("trigger").LPart(qFlag)
var qTotalWaitKey = fdbx.Key("wait").LPart(qFlag)
var qTotalWorkKey = fdbx.Key("work").LPart(qFlag)

// usrValWrapper - преобразователь пользовательского значения в системное, для вставки
func usrValWrapper(tx mvcc.Tx, tbid uint16) fdbx.ValueWrapper {
	mgr := newBLOBKeyManager(tbid)

	return func(v []byte) (_ []byte, err error) {
		mod := models.ValueT{
			Blob: false,
			GZip: false,
			Size: uint32(len(v)),
			Hash: 0,
			Data: v,
		}

		// Достаточно длинное значение, чтобы можно было пытаться сжать его
		if mod.Size > gzLimit {
			buf := zipPool.Get().(*bytes.Buffer)
			gzw := gzip.NewWriter(buf)

			if _, err = gzw.Write(mod.Data); err != nil {
				return nil, ErrValPack.WithReason(err)
			}

			if err = gzw.Close(); err != nil {
				return nil, ErrValPack.WithReason(err)
			}

			mod.GZip = true
			mod.Data = buf.Bytes()
			mod.Size = uint32(len(mod.Data))

			buf.Reset()
			zipPool.Put(buf)
		}

		// Слишком длинное значение, даже после сжатия не влезает в ячейку
		if mod.Size > loLimit {
			uid := typex.NewUUID()

			if err = tx.SaveBLOB(mgr.Wrap(fdbx.Key(uid)), mod.Data); err != nil {
				return nil, ErrValPack.WithReason(err)
			}

			mod.Blob = true
			mod.Data = []byte(uid)
		}

		buf := fbsPool.Get().(*fbs.Builder)
		buf.Finish(mod.Pack(buf))
		res := buf.FinishedBytes()
		buf.Reset()
		fbsPool.Put(buf)
		return res, nil
	}
}

// sysValWrapper - преобразователь системного значения в пользовательское, для выборки
func sysValWrapper(tx mvcc.Tx, tbid uint16) fdbx.ValueWrapper {
	mgr := newBLOBKeyManager(tbid)

	return func(v []byte) (_ []byte, err error) {
		if len(v) == 0 {
			return nil, nil
		}

		var mod models.ValueT
		models.GetRootAsValue(v, 0).UnPackTo(&mod)

		// Если значение лежит в BLOB, надо достать
		if mod.Blob {
			if mod.Data, err = tx.LoadBLOB(mgr.Wrap(fdbx.Key(mod.Data)), int(mod.Size)); err != nil {
				return nil, ErrValUnpack.WithReason(err)
			}
			mod.Blob = false
		}

		// Если значение было сжато, надо расжать
		if mod.GZip {
			var gzr *gzip.Reader

			buf := zipPool.Get().(*bytes.Buffer)

			if gzr, err = gzip.NewReader(bytes.NewReader(mod.Data)); err != nil {
				return nil, ErrValUnpack.WithReason(err)
			}

			if _, err = io.Copy(buf, gzr); err != nil {
				return nil, ErrValUnpack.WithReason(err)
			}

			mod.GZip = false
			mod.Data = buf.Bytes()

			buf.Reset()
			zipPool.Put(buf)
		}

		mod.Size = uint32(len(mod.Data))
		return mod.Data, nil
	}
}
