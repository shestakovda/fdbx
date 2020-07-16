package mvcc_test

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/shestakovda/fdbx/db"
	"github.com/shestakovda/fdbx/mvcc"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const TestDB byte = 0x10

func TestMVCC(t *testing.T) {
	suite.Run(t, new(MVCCSuite))
}

type MVCCSuite struct {
	suite.Suite

	tx mvcc.Tx
	cn db.Connection
}

func (s *MVCCSuite) SetupTest() {
	var err error

	s.cn, err = db.ConnectV610(TestDB)
	s.Require().NoError(err)
	s.Require().NoError(s.cn.Clear())

	s.tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)
}

func (s *MVCCSuite) TearDownTest() {
	s.NoError(s.tx.Cancel())
}

func (s *MVCCSuite) TestUpsertIsolationSameKeys() {
	// test pairs
	var sel mvcc.Value
	key := mvcc.NewStrKey("key1")
	val1 := mvcc.NewStrValue("val1")
	val2 := mvcc.NewStrValue("val2")

	// start tx
	tx1, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)
	tx2, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)

	// insert and check inside tx1
	if err = tx1.Upsert(key, val1); s.NoError(err) {
		if sel, err = tx1.Select(key); s.NoError(err) {
			s.Equal(val1.String(), sel.String())
		}
	}

	// insert and check inside tx2
	if err = tx2.Upsert(key, val2); s.NoError(err) {
		if sel, err = tx2.Select(key); s.NoError(err) {
			s.Equal(val2.String(), sel.String())
		}
	}

	// check no value change inside tx1
	if sel, err = tx1.Select(key); s.NoError(err) {
		s.Equal(val1.String(), sel.String())
	}

	// commit tx2
	s.Require().NoError(tx2.Commit())

	// check there are key change inside tx1
	if sel, err = tx1.Select(key); s.NoError(err) {
		s.Equal(val2.String(), sel.String())
	}
}

func (s *MVCCSuite) TestUpsertIsolationDiffKeys() {
	// test pairs
	var sel mvcc.Value
	key1 := mvcc.NewStrKey("key1")
	key2 := mvcc.NewStrKey("key2")
	val1 := mvcc.NewStrValue("val1")
	val2 := mvcc.NewStrValue("val2")

	// start tx
	tx1, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)
	tx2, err := mvcc.Begin(s.cn)
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

func (s *MVCCSuite) TestUpsertIsolationSameTx() {
	// test pairs
	var err error
	var sel mvcc.Value
	key1 := mvcc.NewStrKey("key1")
	key2 := mvcc.NewStrKey("key2")
	val1 := mvcc.NewStrValue("val1")
	val2 := mvcc.NewStrValue("val2")

	// insert and check val1
	if err = s.tx.Upsert(key1, val1); s.NoError(err) {
		if sel, err = s.tx.Select(key1); s.NoError(err) {
			s.Equal(val1.String(), sel.String())
		}
	}

	// check there are no key2
	if sel, err = s.tx.Select(key2); s.NoError(err) {
		s.Nil(sel)
	}

	// insert and check val2
	if err = s.tx.Upsert(key2, val2); s.NoError(err) {
		if sel, err = s.tx.Select(key2); s.NoError(err) {
			s.Equal(val2.String(), sel.String())
		}
	}

	// delete and check there are no val1
	if err = s.tx.Delete(key1); s.NoError(err) {
		if sel, err = s.tx.Select(key1); s.NoError(err) {
			s.Nil(sel)
		}
	}
}

func (s *MVCCSuite) TestConcurrentInsideTx() {
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()

		for i := 0; i < 100; i++ {
			key := mvcc.NewStrKey(fmt.Sprintf("key%d", i%9))
			val := mvcc.NewStrValue(strconv.Itoa(i))
			s.Require().NoError(s.tx.Upsert(key, val))
		}
	}

	cpu := runtime.NumCPU()
	wg.Add(cpu)

	for i := 0; i < cpu; i++ {
		go worker()
	}

	wg.Wait()
}

func (s *MVCCSuite) TestConcurrentBetweenTx() {
	var wg sync.WaitGroup

	worker := func(n int) {
		defer wg.Done()

		for i := 0; i < 100; i++ {
			tx, err := mvcc.Begin(s.cn)
			s.Require().NoError(err)

			key := mvcc.NewStrKey(fmt.Sprintf("key%d", i%9))
			val := mvcc.NewStrValue(strconv.Itoa(i))
			s.Require().NoError(tx.Upsert(key, val))

			if i%n == 0 {
				s.Require().NoError(tx.Commit())
			} else {
				s.Require().NoError(tx.Cancel())
			}
		}
	}

	cpu := runtime.NumCPU()
	wg.Add(cpu)

	for i := 0; i < cpu; i++ {
		go worker(i + 1)
	}

	wg.Wait()
}

func BenchmarkSequenceWorkflowFourTx(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)
	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := mvcc.NewStrKey(uuid.New().String())
		val := mvcc.NewStrValue(uuid.New().String())
		val2 := mvcc.NewStrValue(uuid.New().String())

		// INSERT
		tx, err := mvcc.Begin(cn)
		require.NoError(b, err)
		require.NoError(b, tx.Upsert(key, val))
		require.NoError(b, tx.Commit())

		// UPDATE
		tx, err = mvcc.Begin(cn)
		require.NoError(b, err)
		require.NoError(b, tx.Upsert(key, val2))
		require.NoError(b, tx.Commit())

		// SELECT / DELETE
		tx, err = mvcc.Begin(cn)
		require.NoError(b, err)
		sl, err := tx.Select(key)
		require.NoError(b, err)
		require.Equal(b, val2.String(), sl.String())
		require.NoError(b, tx.Delete(key))
		require.NoError(b, tx.Commit())

		// SELECT EMPTY
		tx, err = mvcc.Begin(cn)
		require.NoError(b, err)
		sl, err = tx.Select(key)
		require.NoError(b, err)
		require.Nil(b, sl)
		require.NoError(b, tx.Cancel())
	}
}

func BenchmarkSequenceWorkflowSameTx(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)
	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := mvcc.NewStrKey(uuid.New().String())
		val := mvcc.NewStrValue(uuid.New().String())
		val2 := mvcc.NewStrValue(uuid.New().String())

		// INSERT
		tx, err := mvcc.Begin(cn)
		require.NoError(b, err)
		require.NoError(b, tx.Upsert(key, val))

		// UPDATE
		require.NoError(b, tx.Upsert(key, val2))

		// SELECT / DELETE
		sl, err := tx.Select(key)
		require.NoError(b, err)
		require.Equal(b, val2.String(), sl.String())
		require.NoError(b, tx.Delete(key))

		// SELECT EMPTY
		sl, err = tx.Select(key)
		require.NoError(b, err)
		require.Nil(b, sl)
		require.NoError(b, tx.Commit())
	}
}

func BenchmarkOperationsSameTx(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)
	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	key := mvcc.NewStrKey(uuid.New().String())
	val := mvcc.NewStrValue(uuid.New().String())

	tx, err := mvcc.Begin(cn)
	require.NoError(b, err)

	b.ResetTimer()

	b.Run("UPSERT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			require.NoError(b, tx.Upsert(key, val))
		}
	})

	b.Run("SELECT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sl, err := tx.Select(key)
			require.NoError(b, err)
			require.Equal(b, val.String(), sl.String())
		}
	})

	b.Run("DELETE", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			require.NoError(b, tx.Upsert(key, val))
			require.NoError(b, tx.Delete(key))
		}
	})
}
