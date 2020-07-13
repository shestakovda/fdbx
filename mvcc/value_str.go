package mvcc

func NewStrValue(s string) Value {
	return strBytes(s)
}
