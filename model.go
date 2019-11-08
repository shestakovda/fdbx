package fdbx

// Fabric - model fabric func
type Fabric func(id []byte) Model

// Predicat - for query filtering, especially for seq scan queries
type Predicat func(buf []byte) (bool, error)

// Model - abstract database record
type Model interface {
	ID() []byte
	Type() uint16
	Dump() []byte
	Load([]byte) error
	Pack() ([]byte, error)
}
