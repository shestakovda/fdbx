package fdbx

func newMockDB(conn *MockConn) (DB, error) {
	return &MockDB{MockConn: conn}, nil
}

// MockDB - stub DB for unit testing
type MockDB struct{ *MockConn }

// ClearAll - mock
func (db *MockDB) ClearAll() error { return db.FClearAll() }

// Set - mock
func (db *MockDB) Set(ctype uint16, id, value []byte) error { return db.FSet(ctype, id, value) }

// Get - mock
func (db *MockDB) Get(ctype uint16, id []byte) ([]byte, error) { return db.FGet(ctype, id) }

// Del - mock
func (db *MockDB) Del(ctype uint16, id []byte) error { return db.FDel(ctype, id) }

// Save - mock
func (db *MockDB) Save(m ...Model) error { return db.FSave(m...) }

// Load - mock
func (db *MockDB) Load(m ...Model) error { return db.FLoad(m...) }

// Drop - mock
func (db *MockDB) Drop(m ...Model) error { return db.FDrop(m...) }

// Select - mock
func (db *MockDB) Select(ctype uint16, fab Fabric, opts ...Option) ([]Model, error) {
	return db.FSelect(ctype, fab, opts...)
}
