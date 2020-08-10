package mvcc

func NewBytesKey(parts ...[]byte) Value {
	switch len(parts) {
	case 0:
		return nil
	case 1:
		return bytesKey(parts[0])
	default:
		cnt := 0

		for i := range parts {
			cnt += len(parts[i])
		}

		key := make(bytesKey, 0, cnt)

		for i := range parts {
			key = append(key, parts[i]...)
		}

		return key
	}
}

type bytesKey []byte

func (v bytesKey) Bytes() []byte  { return v }
func (v bytesKey) String() string { return string(v) }
