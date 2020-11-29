package mvcc

import (
	"encoding/binary"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
)

type sysPair struct {
	opid uint32
	txid uint64
	orig fdbx.Pair
}

func (p sysPair) Key() fdbx.Key {
	var txid [8]byte
	binary.BigEndian.PutUint64(txid[:], p.txid)
	return WrapKey(p.orig.Key()).RPart(txid[:]...)
}

func (p sysPair) Value() []byte {
	return fdbx.FlatPack(&models.RowT{
		State: &models.RowStateT{
			XMin: p.txid,
			CMin: p.opid,
		},
		Data: p.orig.Value(),
	})
}

func (p sysPair) Unwrap() fdbx.Pair { return p.orig }
