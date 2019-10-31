package fdbx

// Model - abstract database record
type Model interface {
	ID() []byte
	Type() uint16
	Dump() []byte
	Load([]byte) error
	Pack() ([]byte, error)
}
