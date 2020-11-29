package fdbx

type simplePair struct {
	k Key
	v []byte
}

func (p simplePair) Key() Key      { return p.k }
func (p simplePair) Value() []byte { return p.v }
func (p simplePair) Unwrap() Pair  { return nil }
