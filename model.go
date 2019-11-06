package fdbx

// Fabric - model fabric func
type Fabric func(id []byte) Model

// Model - abstract database record
type Model interface {
	ID() []byte
	Type() uint16
	Dump() []byte
	Load([]byte) error
	Pack() ([]byte, error)
}
