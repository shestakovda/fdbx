package mvcc

func NewBytesValue(b []byte) Value {
	return bytesValue(b)
}

type bytesValue []byte

func (v bytesValue) Bytes() []byte  { return v }
func (v bytesValue) String() string { return string(v) }
