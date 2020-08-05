package orm

import "github.com/shestakovda/fdbx/mvcc"

func NewTable(id uint16) Collection {
	return &table{
		id: id,
	}
}

type table struct {
	id uint16
}

func (t *table) ID() uint16 { return t.id }

func (t *table) Upsert(tx mvcc.Tx, m Model) (err error) {
	var val mvcc.Value

	if val, err = m.Pack(); err != nil {
		return ErrUpsert.WithReason(err)
	}

	// TODO: параллельная или массовая загрузка
	if err = tx.Upsert(m.ID(), val); err != nil {
		return ErrUpsert.WithReason(err)
	}

	return nil
}

func (t *table) Select(tx mvcc.Tx) Query {
	return NewTableQuery(t, tx)
}
