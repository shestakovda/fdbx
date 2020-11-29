package orm

import (
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/typex"
)

func newSysPair(tx mvcc.Tx, tbid uint16, orig fdbx.Pair) (_ fdbx.Pair, err error) {
	val := orig.Value()
	mod := &models.ValueT{
		Blob: false,
		Size: uint32(len(val)),
		Hash: 0,
		Data: val,
	}

	// Слишком длинное значение, даже после сжатия не влезает в ячейку
	if len(mod.Data) > loLimit {
		uid := typex.NewUUID()

		if err = tx.SaveBLOB(WrapBlobKey(tbid, fdbx.Bytes2Key(uid)), mod.Data); err != nil {
			return nil, ErrValPack.WithReason(err)
		}

		mod.Blob = true
		mod.Data = []byte(uid)
	}

	return &sysPair{
		tbid: tbid,
		orig: orig,
		data: fdbx.FlatPack(mod),
	}, nil
}

type sysPair struct {
	tbid uint16
	data []byte
	orig fdbx.Pair
}

func (p sysPair) Key() fdbx.Key {
	return WrapTableKey(p.tbid, p.orig.Key())
}

func (p sysPair) Value() []byte {
	return p.data
}

func (p sysPair) Unwrap() fdbx.Pair { return p.orig }
