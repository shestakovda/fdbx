package orm

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func newUsrPair(tx mvcc.Tx, tbid uint16, orig fdb.KeyValue) (_ fdb.KeyValue, err error) {
	val := orig.Value

	if len(val) == 0 {
		return fdb.KeyValue{}, ErrValUnpack.WithStack()
	}

	mod := models.GetRootAsValue(val, 0).UnPack()

	// Если значение лежит в BLOB, надо достать
	if mod.Blob {
		if mod.Data, err = tx.LoadBLOB(WrapBlobKey(tbid, mod.Data), int(mod.Size)); err != nil {
			return fdb.KeyValue{}, ErrValUnpack.WithReason(err)
		}
	}

	return fdb.KeyValue{
		UnwrapTableKey(orig.Key),
		mod.Data,
	}, nil
}
