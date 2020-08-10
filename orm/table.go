package orm

import "github.com/shestakovda/fdbx/mvcc"

func Table(id uint16, fab ModelFabric) Collection {
	return &table{
		id:     id,
		fabric: fab,
	}
}

type table struct {
	id     uint16
	fabric ModelFabric
}

func (t *table) ID() uint16          { return t.id }
func (t *table) Fabric() ModelFabric { return t.fabric }

func (t *table) Upsert(tx mvcc.Tx, mods ...Model) (err error) {
	var val mvcc.Value

	switch len(mods) {
	case 0:
		return nil
	case 1:
		if val, err = mods[0].Pack(); err != nil {
			return ErrUpsert.WithReason(err)
		}

		if err = tx.Upsert(t.SysKey(mods[0].Key()), val); err != nil {
			return ErrUpsert.WithReason(err)
		}

		return nil
	default:
		batch := make([]*mvcc.Pair, len(mods))

		for i := range mods {
			batch[i] = &mvcc.Pair{
				Key: t.SysKey(mods[i].Key()),
			}

			if batch[i].Value, err = mods[i].Pack(); err != nil {
				return ErrUpsert.WithReason(err)
			}
		}

		if err = tx.UpsertBatch(batch...); err != nil {
			return ErrUpsert.WithReason(err)
		}

		return nil
	}
}

func (t *table) Select(tx mvcc.Tx) Query { return NewQuery(t, tx) }

func (t *table) SysKey(usr mvcc.Key) mvcc.Key {
	return mvcc.NewBytesKey([]byte{byte(t.id) >> 8, byte(t.id)}, usr.Bytes())
}

func (t *table) UsrKey(sys mvcc.Key) mvcc.Key {
	sb := sys.Bytes()
	return mvcc.NewBytesKey(sb[2 : len(sb)-8])
}
