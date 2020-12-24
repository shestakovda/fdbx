package mvcc

import (
	"encoding/binary"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
)

type sysPair struct {
	opid uint32
	txid []byte
	orig fdbx.Pair
}

func (p sysPair) Key() fdbx.Key {
	var part [16]byte
	copy(part[:8], p.txid[:8])
	copy(part[12:16], p.txid[8:12])
	binary.BigEndian.PutUint32(part[8:12], p.opid)
	return WrapKey(p.orig.Key()).RPart(part[:]...)
}

func (p sysPair) Value() []byte {
	return fdbx.FlatPack(&models.RowT{
		Data: p.orig.Value(),
	})
}

func (p sysPair) Unwrap() fdbx.Pair { return p.orig }
