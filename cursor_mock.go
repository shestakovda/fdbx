package fdbx

import "context"

func newMockCursor(conn *MockConn, typeID uint16, fab Fabric, start []byte, page int) (*mockCursor, error) {
	return &mockCursor{MockConn: conn, id: typeID, mf: fab, st: start, pg: page}, nil
}

type mockCursor struct {
	*MockConn
	id uint16
	mf Fabric
	pg int
	st []byte
}

// FdbxID
func (m *mockCursor) FdbxID() []byte { return m.FFdbxID() }

// FdbxType
func (m *mockCursor) FdbxType() uint16 { return m.FFdbxType() }

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
func (m *mockCursor) Select(ctx context.Context, opts ...Option) (<-chan Record, <-chan error) {
	return m.FCursorSelect(ctx)
}

// Settings - current settings
func (m *mockCursor) Settings() (uint16, Fabric) { return m.FSettings() }
