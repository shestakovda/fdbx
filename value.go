package fdbx

func (v Value) Getter() Value { return v }

func (v Value) String() string { return string(v) }
