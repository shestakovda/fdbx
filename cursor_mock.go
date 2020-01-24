package fdbx

import "context"

type mockCursor struct {
	*MockConn
}

// FdbxID
func (m *mockCursor) FdbxID() string { return m.FFdbxID() }

// FdbxType
func (m *mockCursor) FdbxType() RecordType { return m.FFdbxType() }

// FdbxIndex
func (m *mockCursor) FdbxIndex(idx Indexer) error { return m.FFdbxIndex(idx) }

// FdbxMarshal
func (m *mockCursor) FdbxMarshal() ([]byte, error) { return m.FFdbxMarshal() }

// FdbxUnmarshal
func (m *mockCursor) FdbxUnmarshal(b []byte) error { return m.FFdbxUnmarshal(b) }

// Empty -
func (m *mockCursor) Empty() bool { return m.FEmpty() }

// Close - mark cursor as empty and drop it from database
func (m *mockCursor) Close() error { return m.FClose() }

// Next - `page` records from collection or index
func (m *mockCursor) Next(db DB, skip uint8) ([]Record, error) { return m.FNext(db, skip) }

// Prev - `page` records from collection or index
func (m *mockCursor) Prev(db DB, skip uint8) ([]Record, error) { return m.FPrev(db, skip) }

// Select all records from current position to the end of collection
func (m *mockCursor) Select(ctx context.Context) (<-chan Record, <-chan error) {
	return m.FCursorSelect(ctx)
}
