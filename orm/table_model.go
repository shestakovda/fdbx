package orm

import "github.com/shestakovda/fdbx/mvcc"

func NewTableModel(id string) Model {
	return &tableModel{
		id: id,
	}
}

type tableModel struct {
	id string
}

func (m *tableModel) ID() mvcc.Key { return mvcc.NewStrKey(m.id) }

func (m *tableModel) Pack() (mvcc.Value, error) {
	return nil, nil
}

func (m *tableModel) Unpack(mvcc.Value) error {
	return nil
}
