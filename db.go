package fdbx

// DB - database object that holds connection for transaction handler
type DB interface {
	ClearAll() error

	Set(ctype uint16, id, value []byte) error
	Get(ctype uint16, id []byte) ([]byte, error)
	Del(ctype uint16, id []byte) error

	Save(...Model) error
	Load(...Model) error
	Drop(...Model) error

	Select(ctype uint16, fab Fabric, opts ...Option) ([]Model, error)
}
