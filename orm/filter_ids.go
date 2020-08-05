package orm

func NewIDsFilter(ids ...string) Filter {
	return &idsFilter{
		ids: ids,
	}
}

type idsFilter struct {
	ids []string
}

func (f *idsFilter) Skip(m Model) (bool, error) {
	return false, nil
}
