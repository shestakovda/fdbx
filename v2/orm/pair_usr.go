package orm

import (
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func newUsrPair(tx mvcc.Tx, tbid uint16, orig fdbx.Pair) (_ fdbx.Pair, err error) {
	val := orig.Value()

	if len(val) == 0 {
		return nil, ErrValUnpack.WithStack()
	}

	mod := models.GetRootAsValue(val, 0).UnPack()

	// Если значение лежит в BLOB, надо достать
	if mod.Blob {
		if mod.Data, err = tx.LoadBLOB(WrapBlobKey(tbid, fdbx.Bytes2Key(mod.Data)), int(mod.Size)); err != nil {
			return nil, ErrValUnpack.WithReason(err)
		}
	}

	return &usrPair{
		data: mod.Data,
		orig: orig,
	}, nil
}

type usrPair struct {
	data []byte
	orig fdbx.Pair
}

func (p usrPair) Key() fdbx.Key {
	return UnwrapTableKey(p.orig.Key())
}

func (p usrPair) Value() []byte {
	return p.data
}

func (p usrPair) Unwrap() fdbx.Pair { return p.orig }
