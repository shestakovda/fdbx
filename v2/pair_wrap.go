package fdbx

type wrapPair struct {
	k Key
	p Pair
}

func (p wrapPair) Key() Key      { return p.k }
func (p wrapPair) Value() []byte { return p.p.Value() }
func (p wrapPair) Unwrap() Pair  { return p.p }
