package mvcc

import "github.com/shestakovda/fdbx/models"

type rowValue struct {
	*models.Row
}

func (r rowValue) Bytes() []byte  { return r.DataBytes() }
func (r rowValue) String() string { return string(r.DataBytes()) }
