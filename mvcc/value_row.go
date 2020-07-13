package mvcc

import "github.com/shestakovda/fdbx/models"

type rowValue struct {
	*models.Row
}

func (r *rowValue) Bytes() []byte {
	buf := r.DataBytes()

	// if r.Blob() {

	// }

	// if r.GZip() {

	// }

	return buf
}
