package mvcc

import (
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
)

type sysPair struct {
	opid uint32
	txid []byte
	orig fdbx.Pair
}

func (p sysPair) Key() fdbx.Key {
	return WrapKey(p.orig.Key()).RPart(p.txid...)
}

func (p sysPair) Value() []byte {
	return fdbx.FlatPack(&models.RowT{
		XMin: p.txid,
		CMin: p.opid,
		Data: p.orig.Value(),
	})
}

func (p sysPair) Unwrap() fdbx.Pair { return p.orig }
