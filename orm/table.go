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

func (t *table) Upsert(tx mvcc.Tx, m Model) (err error) {
	var val mvcc.Value

	if val, err = m.Pack(); err != nil {
		return ErrUpsert.WithReason(err)
	}

	// TODO: параллельная или массовая загрузка
	if err = tx.Upsert(t.ID2Key(m.ID()), val); err != nil {
		return ErrUpsert.WithReason(err)
	}

	return nil
}

func (t *table) Select(tx mvcc.Tx) Query { return NewQuery(t, tx) }

func (t *table) ID2Key(id mvcc.Key) mvcc.Key {
	return mvcc.NewBytesKey([]byte{byte(t.id) >> 8, byte(t.id)}, id.Bytes())
}

func (t *table) Key2ID(key mvcc.Key) mvcc.Key {
	return mvcc.NewBytesKey(key.Bytes()[2:])
}
