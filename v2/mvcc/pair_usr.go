package mvcc

import (
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
)

type usrPair struct {
	orig fdbx.Pair
}

func (p usrPair) Key() fdbx.Key {
	return UnwrapKey(p.orig.Key())
}

func (p usrPair) Value() []byte {
	if buf := p.orig.Value(); buf != nil {
		return models.GetRootAsRow(buf, 0).DataBytes()
	}
	return nil
}

func (p usrPair) Unwrap() fdbx.Pair { return p.orig }
