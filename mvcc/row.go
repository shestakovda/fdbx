package mvcc

import "github.com/shestakovda/fdbx/models"

func newRow(buf []byte) *row {
	return &row{
		Row: models.GetRootAsRow(buf, 0),
	}
}

type row struct {
	*models.Row
}

func (r *row) Value() []byte {
	buf := r.DataBytes()

	if r.Blob() {

	}

	if r.GZip() {

	}

	return buf
}
