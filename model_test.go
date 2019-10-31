package fdbx_test

type testModel struct {
	err   error
	key   string
	ctype uint16
	data  []byte
}

func (m *testModel) ID() []byte            { return []byte(m.key) }
func (m *testModel) Type() uint16          { return m.ctype }
func (m *testModel) Dump() []byte          { return m.data }
func (m *testModel) Load(d []byte) error   { m.data = d; return m.err }
func (m *testModel) Pack() ([]byte, error) { return m.data, m.err }
