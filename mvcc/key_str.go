package mvcc

func NewStrKey(s string) Key {
	return strBytes(s)
}
