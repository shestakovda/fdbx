package fdbx

func NewPair(k Key, v Value) Pair {
	p := pair{
		k:  k,
		v:  v,
		kc: make([]KeyWrapper, 0, 4),
		vc: make([]ValueWrapper, 0, 4),
	}
	return &p
}

type pair struct {
	k  Key
	v  Value
	kc []KeyWrapper
	vc []ValueWrapper
}

func (p pair) Key() Key {
	var kcl int

	if kcl = len(p.kc); kcl == 0 {
		return p.k
	}

	k := p.k

	for i := range p.kc {
		k = p.kc[i](k)
	}

	return k
}

func (p pair) Value() Value {
	var vcl int

	if vcl = len(p.vc); vcl == 0 {
		return p.v
	}

	v := p.v

	for i := range p.vc {
		v = p.vc[i](v)
	}

	return v
}

func (p *pair) WrapKey(w KeyWrapper) Pair {
	p.kc = append(p.kc, w)
	return p
}

func (p *pair) WrapValue(w ValueWrapper) Pair {
	p.vc = append(p.vc, w)
	return p
}
