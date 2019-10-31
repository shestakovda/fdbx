package fdbx

// DB - database object that holds connection for transaction handler
type DB interface {
	Set(ctype uint16, id, value []byte) error
	Get(ctype uint16, id []byte) ([]byte, error)
	Del(ctype uint16, id []byte) error

	// Load(...Model) error
	// Save(...Model) error

	// Select(ctx context.Context, ctype uint16, c Cursor, opts ...Option) (<-chan Model, error)
}
