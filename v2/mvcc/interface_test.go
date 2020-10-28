package mvcc_test

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/typex"
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

	mvcc.TxCacheSize = 4

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
	var sel fdbx.Pair
	var val []byte
	key := fdbx.Key("key1")
	val1 := []byte("val1")
	val2 := []byte("val2")

	// start tx
	tx1, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)
	tx2, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)

	// insert and check inside tx1
	if err = tx1.Upsert([]fdbx.Pair{fdbx.NewPair(key, val1)}); s.NoError(err) {
		if sel, err = tx1.Select(key); s.NoError(err) {
			if val, err = sel.Value(); s.NoError(err) {
				s.Equal(string(val1), string(val))
			}
		}
	}

	// insert and check inside tx2
	if err = tx2.Upsert([]fdbx.Pair{fdbx.NewPair(key, val2)}); s.NoError(err) {
		if sel, err = tx2.Select(key); s.NoError(err) {
			if val, err = sel.Value(); s.NoError(err) {
				s.Equal(string(val2), string(val))
			}
		}
	}

	// check no value change inside tx1
	if sel, err = tx1.Select(key); s.NoError(err) {
		if val, err = sel.Value(); s.NoError(err) {
			s.Equal(string(val1), string(val))
		}
	}

	// commit tx2
	s.Require().NoError(tx2.Commit())

	// check there are key change inside tx1
	if sel, err = tx1.Select(key); s.NoError(err) {
		if val, err = sel.Value(); s.NoError(err) {
			s.Equal(string(val2), string(val))
		}
	}
}

func (s *MVCCSuite) TestUpsertIsolationDiffKeys() {
	// test pairs
	var sel fdbx.Pair
	var val []byte
	key1 := fdbx.Key("key1")
	key2 := fdbx.Key("key2")
	val1 := []byte("val1")
	val2 := []byte("val2")

	// start tx
	tx1, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)
	tx2, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)

	// insert and check inside tx1
	if err = tx1.Upsert([]fdbx.Pair{fdbx.NewPair(key1, val1)}); s.NoError(err) {
		if sel, err = tx1.Select(key1); s.NoError(err) {
			if val, err = sel.Value(); s.NoError(err) {
				s.Equal(string(val1), string(val))
			}
		}
	}

	// insert and check inside tx2
	if err = tx2.Upsert([]fdbx.Pair{fdbx.NewPair(key2, val2)}); s.NoError(err) {
		if sel, err = tx2.Select(key2); s.NoError(err) {
			if val, err = sel.Value(); s.NoError(err) {
				s.Equal(string(val2), string(val))
			}
		}
	}

	// check no key1 inside tx2
	if _, err = tx2.Select(key1); s.Error(err) {
		s.True(errx.Is(err, mvcc.ErrSelect))
		s.True(errx.Is(err, mvcc.ErrNotFound))
	}

	// check no key2 inside tx1
	if _, err = tx1.Select(key2); s.Error(err) {
		s.True(errx.Is(err, mvcc.ErrSelect))
		s.True(errx.Is(err, mvcc.ErrNotFound))
	}

	// commit tx2
	s.Require().NoError(tx2.Commit())

	// check there are key2 inside tx1
	if sel, err = tx1.Select(key2); s.NoError(err) {
		if val, err = sel.Value(); s.NoError(err) {
			s.Equal(string(val2), string(val))
		}
	}
}

func (s *MVCCSuite) TestUpsertIsolationSameTx() {
	// test pairs
	var err error
	var sel fdbx.Pair
	var val []byte
	key1 := fdbx.Key("key1")
	key2 := fdbx.Key("key2")
	val1 := []byte("val1")
	val2 := []byte("val2")

	// insert and check val1
	if err = s.tx.Upsert([]fdbx.Pair{fdbx.NewPair(key1, val1)}); s.NoError(err) {
		if sel, err = s.tx.Select(key1); s.NoError(err) {
			if val, err = sel.Value(); s.NoError(err) {
				s.Equal(string(val1), string(val))
			}
		}
	}

	// check there are no key2
	if _, err = s.tx.Select(key2); s.Error(err) {
		s.True(errx.Is(err, mvcc.ErrSelect))
		s.True(errx.Is(err, mvcc.ErrNotFound))
	}

	hdlr := func(tx mvcc.Tx, p fdbx.Pair) error {
		if key, err := p.Key(); s.NoError(err) {
			s.Equal(key2.String(), key.String())
		}
		return nil
	}

	// insert and check val2
	if err = s.tx.Upsert([]fdbx.Pair{fdbx.NewPair(key2, val2)}, mvcc.OnInsert(hdlr)); s.NoError(err) {
		if sel, err = s.tx.Select(key2); s.NoError(err) {
			if val, err = sel.Value(); s.NoError(err) {
				s.Equal(string(val2), string(val))
			}
		}
	}

	hdlr = func(tx mvcc.Tx, p fdbx.Pair) error {
		if key, err := p.Key(); s.NoError(err) {
			s.Equal(key1.String(), key.String())
		}
		return nil
	}

	// delete and check there are no val1
	if err = s.tx.Delete([]fdbx.Key{key1}, mvcc.OnDelete(hdlr)); s.NoError(err) {
		if _, err = s.tx.Select(key1); s.Error(err) {
			s.True(errx.Is(err, mvcc.ErrSelect))
			s.True(errx.Is(err, mvcc.ErrNotFound))
		}
	}
}

func (s *MVCCSuite) TestConcurrentInsideTx() {
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()

		for i := 0; i < 100; i++ {
			pair := fdbx.NewPair([]byte(fmt.Sprintf("key%d", i%9)), []byte(strconv.Itoa(i)))
			s.Require().NoError(s.tx.Upsert([]fdbx.Pair{pair}))
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
			pair := fdbx.NewPair([]byte(fmt.Sprintf("key%d", i%9)), []byte(strconv.Itoa(i)))
			s.Require().NoError(tx.Upsert([]fdbx.Pair{pair}))

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

func (s *MVCCSuite) TestOnCommit() {
	tx, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)

	tx.OnCommit(func(w db.Writer) error {
		return mvcc.ErrBLOBDrop.WithStack()
	})

	if err = tx.Commit(); s.Error(err) {
		s.True(errx.Is(err, mvcc.ErrBLOBDrop, mvcc.ErrClose))
	}
	s.Require().NoError(tx.Cancel())

	tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)

	var num int64 = 123
	var key = fdbx.Key("key")

	tx.OnCommit(func(w db.Writer) error {
		w.Increment(key, num)
		return nil
	})

	s.Require().NoError(s.cn.Write(func(w db.Writer) error {
		s.Require().NoError(tx.Commit(mvcc.Writer(w)))
		if val, err := w.Data(key).Value(); s.NoError(err) {
			s.Equal(num, int64(binary.LittleEndian.Uint64(val)))
		}
		return nil
	}))
}

func (s *MVCCSuite) TestBLOB() {
	key := fdbx.Key("test blob")

	msg := make([]byte, 20<<20)
	_, err := rand.Read(msg)
	s.Require().NoError(err)

	// insert and check long msg
	if err := s.tx.SaveBLOB(key, msg); s.NoError(err) {
		if val, err := s.tx.LoadBLOB(key, len(msg)); s.NoError(err) {
			s.Equal(msg, val)
		}

		if err := s.tx.DropBLOB(key); s.NoError(err) {
			if val, err := s.tx.LoadBLOB(key, len(msg)); s.NoError(err) {
				s.Len(val, 0)
			}
		}
	}
}

func (s *MVCCSuite) TestListAll() {
	key1 := fdbx.Key("key1")
	key2 := fdbx.Key("key2")
	key3 := fdbx.Key("key3")
	key4 := fdbx.Key("key4")
	key5 := fdbx.Key("key5")
	key6 := fdbx.Key("key6")
	key7 := fdbx.Key("key7")
	val1 := []byte("val1")
	val2 := []byte("val2")
	val3 := []byte("val3")
	val4 := []byte("val4")
	val5 := []byte("val5")
	val6 := []byte("val6")
	val7 := []byte("val7")

	s.Require().NoError(s.tx.Upsert([]fdbx.Pair{
		fdbx.NewPair(key1, val1),
		fdbx.NewPair(key2, val2),
		fdbx.NewPair(key3, val3),
		fdbx.NewPair(key4, val4),
		fdbx.NewPair(key5, val5),
		fdbx.NewPair(key6, val6),
		fdbx.NewPair(key7, val7),
	}))

	if list, err := s.tx.ListAll(nil, nil); s.NoError(err) {
		s.Len(list, 7)
	}

	if list, err := s.tx.ListAll(key2, key6); s.NoError(err) {
		s.Len(list, 5)
	}
	s.Require().NoError(s.tx.Commit())

	// Удаляем парочку и коммитим это

	tx, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)
	s.Require().NoError(tx.Delete([]fdbx.Key{key3, key5}))
	s.Require().NoError(tx.Commit())

	// Удаляем еще парочку, но не коммитим

	tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)
	s.Require().NoError(tx.Delete([]fdbx.Key{key4, key6}))

	// В отдельной транзакции запрашиваем результат

	vals := make([]string, 0, 2)
	hdlr := func(tx mvcc.Tx, p fdbx.Pair, w db.Writer) error {
		if val, err := p.Value(); s.NoError(err) {
			vals = append(vals, string(val))
		}
		return nil
	}

	tx2, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)

	s.Require().NoError(s.cn.Write(func(w db.Writer) error {
		if list, err := tx2.ListAll(
			key2, nil,
			mvcc.Limit(3),
			mvcc.PackSize(2),
			mvcc.Writer(w),
			mvcc.Exclusive(hdlr),
		); s.NoError(err) {
			s.Len(list, 3)
			s.Equal([]string{"val2", "val4", "val6"}, vals)
		}

		return nil
	}))
	s.Require().NoError(tx.Cancel())
	s.Require().NoError(tx2.Cancel())
}

func BenchmarkSequenceWorkflowFourTx(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)
	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fdbx.Key(typex.NewUUID())
		val := []byte(typex.NewUUID().String())
		val2 := []byte(typex.NewUUID().String())

		// INSERT
		tx, err := mvcc.Begin(cn)
		require.NoError(b, err)
		require.NoError(b, tx.Upsert([]fdbx.Pair{fdbx.NewPair(key, val)}))
		require.NoError(b, tx.Commit())

		// UPDATE
		tx, err = mvcc.Begin(cn)
		require.NoError(b, err)
		require.NoError(b, tx.Upsert([]fdbx.Pair{fdbx.NewPair(key, val2)}))
		require.NoError(b, tx.Commit())

		// SELECT / DELETE
		tx, err = mvcc.Begin(cn)
		require.NoError(b, err)
		sl, err := tx.Select(key)
		require.NoError(b, err)
		vv, err := sl.Value()
		require.NoError(b, err)
		require.Equal(b, string(val2), string(vv))
		require.NoError(b, tx.Delete([]fdbx.Key{key}))
		require.NoError(b, tx.Commit())

		// SELECT EMPTY
		tx, err = mvcc.Begin(cn)
		require.NoError(b, err)
		sl, err = tx.Select(key)
		require.Error(b, err)
		require.True(b, errx.Is(err, mvcc.ErrNotFound))
		require.NoError(b, tx.Cancel())
	}
}

func BenchmarkSequenceWorkflowSameTx(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)
	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fdbx.Key(typex.NewUUID())
		val := []byte(typex.NewUUID().String())
		val2 := []byte(typex.NewUUID().String())

		// INSERT
		tx, err := mvcc.Begin(cn)
		require.NoError(b, err)
		require.NoError(b, tx.Upsert([]fdbx.Pair{fdbx.NewPair(key, val)}))

		// UPDATE
		require.NoError(b, tx.Upsert([]fdbx.Pair{fdbx.NewPair(key, val2)}))

		// SELECT / DELETE
		sl, err := tx.Select(key)
		require.NoError(b, err)
		vv, err := sl.Value()
		require.NoError(b, err)
		require.Equal(b, string(val2), string(vv))
		require.NoError(b, tx.Delete([]fdbx.Key{key}))

		// SELECT EMPTY
		sl, err = tx.Select(key)
		require.Error(b, err)
		require.True(b, errx.Is(err, mvcc.ErrNotFound))
		require.NoError(b, tx.Commit())
	}
}

func BenchmarkOperationsSameTx(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)
	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	uid := []byte(typex.NewUUID())
	key := fdbx.Key(uid)
	val := uid

	tx, err := mvcc.Begin(cn)
	require.NoError(b, err)
	require.NoError(b, tx.Upsert([]fdbx.Pair{fdbx.NewPair(uid, uid)}))

	b.ResetTimer()

	b.Run("UPSERT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			uid := []byte(typex.NewUUID())
			require.NoError(b, tx.Upsert([]fdbx.Pair{fdbx.NewPair(uid, uid)}))
		}
	})

	b.Run("SELECT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sl, err := tx.Select(key)
			require.NoError(b, err)
			vv, err := sl.Value()
			require.NoError(b, err)
			require.Equal(b, string(val), string(vv))
		}
	})

	b.Run("DELETE", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			uid := []byte(typex.NewUUID())
			require.NoError(b, tx.Upsert([]fdbx.Pair{fdbx.NewPair(uid, uid)}))
			require.NoError(b, tx.Delete([]fdbx.Key{uid}))
		}
	})
}
