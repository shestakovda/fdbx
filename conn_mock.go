package fdbx

func newMockConn(db uint16) (conn *MockConn, err error) {
	return &MockConn{baseConn: newBaseConn(db)}, nil
}

// MockConn - stub conn for unit testing
type MockConn struct {
	*baseConn

	FClearDB func() error
	FTx      func(TxHandler) error
	FQueue   func(uint16, Fabric) Queue
}

// ClearDB - clear stub, set FClearDB before usage
func (m *MockConn) ClearDB() error { return m.FClearDB() }

// Tx - tx stub, set FTx before usage
func (m *MockConn) Tx(h TxHandler) error { return m.FTx(h) }

// Queue - clear stub, set FQueue before usage
func (m *MockConn) Queue(qtype uint16, f Fabric) Queue { return m.FQueue(qtype, f) }
