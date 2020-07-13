package mvcc_test

import (
	"testing"

	"github.com/shestakovda/fdbx/db"
	"github.com/shestakovda/fdbx/mvcc"
	"github.com/stretchr/testify/suite"
)

const TestDB byte = 1

func TestMVCC(t *testing.T) {
	suite.Run(t, new(MVCCSuite))
}

type MVCCSuite struct {
	suite.Suite

	tx mvcc.Tx
}

func (s *MVCCSuite) SetupTest() {
	cn, err := db.ConnectV610(TestDB)
	s.Require().NoError(err)

	s.tx, err = mvcc.Begin(cn)
	s.Require().NoError(err)
}

func (s *MVCCSuite) TearDownTest() {
	s.NoError(s.tx.Cancel())
}

func (s *MVCCSuite) TestWorkflow() {
	var err error
	var v1, v2 mvcc.Value

	key := mvcc.NewStrKey("key1")

	if v1, err = s.tx.Select(key); s.NoError(err) {
		s.Nil(v1)
	}

	v1 = mvcc.NewStrValue("value1")

	if err = s.tx.Upsert(key, v1); s.NoError(err) {
		if v2, err = s.tx.Select(key); s.NoError(err) {
			s.Equal(v1.String(), v2.String())
		}
	}

	if err = s.tx.Delete(key); s.NoError(err) {
		if v2, err = s.tx.Select(key); s.NoError(err) {
			s.Nil(v2)
		}
	}

}

func (s *MVCCSuite) TestUpsertIsolation() {
	// test pairs
	var sel mvcc.Value
	key1 := mvcc.NewStrKey("key1")
	key2 := mvcc.NewStrKey("key2")
	val1 := mvcc.NewStrValue("val1")
	val2 := mvcc.NewStrValue("val2")

	// connect
	cn, err := db.ConnectV610(TestDB)
	s.Require().NoError(err)

	// start tx
	tx1, err := mvcc.Begin(cn)
	s.Require().NoError(err)
	tx2, err := mvcc.Begin(cn)
	s.Require().NoError(err)

	// insert and check inside tx1
	if err = tx1.Upsert(key1, val1); s.NoError(err) {
		if sel, err = tx1.Select(key1); s.NoError(err) {
			s.Equal(val1.String(), sel.String())
		}
	}

	// insert and check inside tx2
	if err = tx2.Upsert(key2, val2); s.NoError(err) {
		if sel, err = tx2.Select(key2); s.NoError(err) {
			s.Equal(val2.String(), sel.String())
		}
	}

	// check no key1 inside tx2
	if sel, err = tx2.Select(key1); s.NoError(err) {
		s.Nil(sel)
	}

	// check no key2 inside tx1
	if sel, err = tx1.Select(key2); s.NoError(err) {
		s.Nil(sel)
	}

	// commit tx2
	s.Require().NoError(tx2.Commit())

	// check there are key2 inside tx1
	if sel, err = tx1.Select(key2); s.NoError(err) {
		s.Equal(val2.String(), sel.String())
	}
}
