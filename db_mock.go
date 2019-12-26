package fdbx

func newMockDB(conn *MockConn) *mockDB { return &mockDB{MockConn: conn} }

type mockDB struct{ *MockConn }

func (db *mockDB) Set(ctype uint16, id, value []byte) error    { return db.FSet(ctype, id, value) }
func (db *mockDB) Get(ctype uint16, id []byte) ([]byte, error) { return db.FGet(ctype, id) }
func (db *mockDB) Del(ctype uint16, id []byte) error           { return db.FDel(ctype, id) }
func (db *mockDB) Save(h RecordHandler, m ...Record) error     { return db.FSave(h, m...) }
func (db *mockDB) Load(h RecordHandler, m ...Record) error     { return db.FLoad(h, m...) }
func (db *mockDB) Drop(h RecordHandler, m ...Record) error     { return db.FDrop(h, m...) }
func (db *mockDB) Index(h IndexHandler, rid []byte, drop bool) (err error) {
	return db.FIndex(h, rid, drop)
}
func (db *mockDB) Clear(typeID uint16) error {
	return db.FClear(typeID)
}
func (db *mockDB) ClearIndex(h IndexHandler) error {
	return db.FClearIndex(h)
}
func (db *mockDB) Select(rtp RecordType, opts ...Option) ([]Record, error) {
	return db.FSelect(rtp, opts...)
}
func (db *mockDB) SelectIDs(typeID uint16, opts ...Option) ([][]byte, error) {
	return db.FSelectIDs(typeID, opts...)
}
