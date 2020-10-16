package fdbx

func NewPair(k Key, v []byte) Pair {
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
	v  []byte
	kc []KeyWrapper
	vc []ValueWrapper
}

func (p pair) Key() (k Key, err error) {
	var kcl int

	if kcl = len(p.kc); kcl == 0 {
		return p.k, nil
	}

	k = p.k

	for i := range p.kc {
		if k, err = p.kc[i](k); err != nil {
			return nil, ErrKey.WithReason(err)
		}
	}

	return k, nil
}

func (p pair) Value() (v []byte, err error) {
	var vcl int

	if vcl = len(p.vc); vcl == 0 {
		return p.v, nil
	}

	v = p.v

	for i := range p.vc {
		if v, err = p.vc[i](v); err != nil {
			return nil, ErrValue.WithReason(err)
		}
	}

	return v, nil
}

func (p *pair) WrapKey(w KeyWrapper) Pair {
	p.kc = append(p.kc, w)
	return p
}

func (p *pair) WrapValue(w ValueWrapper) Pair {
	p.vc = append(p.vc, w)
	return p
}
