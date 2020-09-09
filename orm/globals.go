package orm

import (
	"sync"

	fbs "github.com/google/flatbuffers/go"

	"github.com/shestakovda/fdbx"
	"github.com/shestakovda/fdbx/models"
)

var fbsPool = sync.Pool{New: func() interface{} { return fbs.NewBuilder(128) }}

// usrKeyWrapper - преобразователь пользовательского ключа в системный, для вставки
func usrKeyWrapper(clid uint16) fdbx.KeyWrapper {
	return func(key fdbx.Key) fdbx.Key { return key.LPart(byte(clid>>8), byte(clid)) }
}

// sysKeyWrapper - преобразователь системного ключа в пользовательский, для выборки
func sysKeyWrapper(key fdbx.Key) fdbx.Key { return key.LSkip(2).RSkip(8) }

// idxKeyWrapper - преобразователь пользовательского ключа в ключ индекса
func idxKeyWrapper(idxid byte) fdbx.KeyWrapper {
	return func(key fdbx.Key) fdbx.Key { return key.LPart(idxid) }
}

// usrValWrapper - преобразователь пользовательского значения в системное, для вставки
func usrValWrapper(v fdbx.Value) fdbx.Value {
	// TODO: преобразователи gzip/blob
	mod := models.ValueT{
		Blob: false,
		GZip: false,
		Hash: 0,
		Data: v,
	}

	buf := fbsPool.Get().(*fbs.Builder)
	buf.Finish(mod.Pack(buf))
	res := buf.FinishedBytes()
	buf.Reset()
	fbsPool.Put(buf)
	return res
}

// sysValWrapper - преобразователь системного значения в пользовательское, для выборки
func sysValWrapper(v fdbx.Value) fdbx.Value {
	if v == nil {
		return nil
	}

	var mod models.ValueT
	models.GetRootAsValue(v, 0).UnPackTo(&mod)
	// TODO: преобразователи gzip/blob
	return mod.Data
}
