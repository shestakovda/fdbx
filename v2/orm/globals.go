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
)

var gzLimit = 840
var zipPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}
var fbsPool = sync.Pool{New: func() interface{} { return fbs.NewBuilder(128) }}

// usrKeyWrapper - преобразователь пользовательского ключа в системный, для вставки
func usrKeyWrapper(clid uint16) fdbx.KeyWrapper {
	return func(key fdbx.Key) (fdbx.Key, error) {
		return key.LPart(byte(clid>>8), byte(clid)), nil
	}
}

// sysKeyWrapper - преобразователь системного ключа в пользовательский, для выборки
func sysKeyWrapper(key fdbx.Key) (fdbx.Key, error) {
	return key.LSkip(2).RSkip(8), nil
}

// idxKeyWrapper - преобразователь пользовательского ключа в ключ индекса
func idxKeyWrapper(idxid byte) fdbx.KeyWrapper {
	return func(key fdbx.Key) (fdbx.Key, error) {
		return key.LPart(idxid), nil
	}
}

// usrValWrapper - преобразователь пользовательского значения в системное, для вставки
func usrValWrapper(tx mvcc.Tx) fdbx.ValueWrapper {
	return func(v fdbx.Value) (_ fdbx.Value, err error) {
		// TODO: преобразователи gzip/blob
		mod := models.ValueT{
			Blob: false,
			GZip: false,
			Hash: 0,
			Data: v,
		}

		// Достаточно длинное значение, чтобы можно было пытаться сжать его
		if len(mod.Data) > gzLimit {
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

			buf.Reset()
			zipPool.Put(buf)
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
func sysValWrapper(v fdbx.Value) (_ fdbx.Value, err error) {
	if v == nil {
		return nil, nil
	}

	var mod models.ValueT
	models.GetRootAsValue(v, 0).UnPackTo(&mod)

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

	// TODO: преобразователи gzip/blob
	return mod.Data, nil
}
