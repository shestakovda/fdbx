package orm

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/typex"
)

func newSysPair(tx mvcc.Tx, tbid uint16, orig fdb.KeyValue) (_ fdb.KeyValue, err error) {
	val := orig.Value
	mod := &models.ValueT{
		Blob: false,
		Size: uint32(len(val)),
		Data: val,
	}

	// Слишком длинное значение, даже после сжатия не влезает в ячейку
	if len(mod.Data) > loLimit {
		uid := typex.NewUUID()

		if err = tx.SaveBLOB(WrapBlobKey(tbid, fdb.Key(uid)), mod.Data); err != nil {
			return fdb.KeyValue{}, ErrValPack.WithReason(err)
		}

		mod.Blob = true
		mod.Data = uid
	}

	return fdb.KeyValue{
		WrapTableKey(tbid, orig.Key),
		fdbx.FlatPack(mod),
	}, nil
}
