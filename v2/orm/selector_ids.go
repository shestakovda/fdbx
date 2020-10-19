package orm

import (
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewIDsSelector(tx mvcc.Tx, ids []fdbx.Key, strict bool) Selector {
	s := idsSelector{
		tx:     tx,
		ids:    ids,
		strict: strict,
	}
	return &s
}

type idsSelector struct {
	tx     mvcc.Tx
	ids    []fdbx.Key
	strict bool
}

func (s idsSelector) Select(tb Table) (list []fdbx.Pair, err error) {
	var pair fdbx.Pair

	wrp := sysValWrapper(s.tx, tb.ID())
	list = make([]fdbx.Pair, 0, len(s.ids))

	for i := range s.ids {
		if pair, err = s.tx.Select(usrKey(tb.ID(), s.ids[i])); err != nil {
			if errx.Is(err, mvcc.ErrNotFound) {
				if !s.strict {
					continue
				}

				err = ErrNotFound.WithReason(err).WithDebug(errx.Debug{
					"id": s.ids[i].String(),
				})
			}

			return nil, ErrSelect.WithReason(err)
		}

		list = append(list, pair.WrapKey(sysKeyWrapper).WrapValue(wrp))
	}

	return list, nil
}
