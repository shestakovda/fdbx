package fdbx

func newMockDB(conn *MockConn) *mockDB { return &mockDB{MockConn: conn} }

type mockDB struct{ *MockConn }

func (db *mockDB) Set(ctype uint16, id, value []byte) error    { return db.FSet(ctype, id, value) }
func (db *mockDB) Get(ctype uint16, id []byte) ([]byte, error) { return db.FGet(ctype, id) }
func (db *mockDB) Del(ctype uint16, id []byte) error           { return db.FDel(ctype, id) }
func (db *mockDB) Save(m ...Record) error                      { return db.FSave(m...) }
func (db *mockDB) Load(m ...Record) error                      { return db.FLoad(m...) }
func (db *mockDB) Drop(m ...Record) error                      { return db.FDrop(m...) }
func (db *mockDB) Index(h IndexHandler, rid []byte, drop bool) (err error) {
	return db.FIndex(h, rid, drop)
}
func (db *mockDB) Select(rtp RecordType, opts ...Option) ([]Record, error) {
	return db.FSelect(rtp, opts...)
}
