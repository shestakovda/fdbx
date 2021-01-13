package mvcc_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/shestakovda/errx"
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

	s.cn, err = db.Connect(TestDB)
	s.Require().NoError(err)
	s.Require().NoError(s.cn.Clear())

	s.tx = mvcc.Begin(s.cn)
}

func (s *MVCCSuite) TearDownTest() {
	s.NoError(s.tx.Cancel())
}

func (s *MVCCSuite) TestUpsertIsolationSameKeys() {
	var err error
	var sel fdb.KeyValue

	key := fdb.Key("key1")
	val1 := []byte("val1")
	val2 := []byte("val2")

	// start tx
	tx1 := mvcc.Begin(s.cn)
	tx2 := mvcc.Begin(s.cn)

	// insert and check inside tx1
	if err = tx1.Upsert([]fdb.KeyValue{{key, val1}}); s.NoError(err) {
		if sel, err = tx1.Select(key); s.NoError(err) {
			s.Equal(string(val1), string(sel.Value))
		}
	}

	// insert and check inside tx2
	if err = tx2.Upsert([]fdb.KeyValue{{key, val2}}); s.NoError(err) {
		if sel, err = tx2.Select(key); s.NoError(err) {
			s.Equal(string(val2), string(sel.Value))
		}
	}

	// check no value change inside tx1
	if sel, err = tx1.Select(key); s.NoError(err) {
		s.Equal(string(val1), string(sel.Value))
	}

	// commit tx2
	s.Require().NoError(tx2.Commit())

	// check there are key change inside tx1
	if sel, err = tx1.Select(key); s.NoError(err) {
		s.Equal(string(val2), string(sel.Value))
	}
}

func (s *MVCCSuite) TestUpsertIsolationDiffKeys() {
	var err error
	var sel fdb.KeyValue

	key1 := fdb.Key("key1")
	key2 := fdb.Key("key2")
	val1 := []byte("val1")
	val2 := []byte("val2")

	// start tx
	tx1 := mvcc.Begin(s.cn)
	tx2 := mvcc.Begin(s.cn)

	// insert and check inside tx1
	if err = tx1.Upsert([]fdb.KeyValue{{key1, val1}}); s.NoError(err) {
		if sel, err = tx1.Select(key1); s.NoError(err) {
			s.Equal(string(val1), string(sel.Value))
		}
	}

	// insert and check inside tx2
	if err = tx2.Upsert([]fdb.KeyValue{{key2, val2}}); s.NoError(err) {
		if sel, err = tx2.Select(key2); s.NoError(err) {
			s.Equal(string(val2), string(sel.Value))
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
		s.Equal(string(val2), string(sel.Value))
	}
}

func (s *MVCCSuite) TestUpsertIsolationSameTx() {
	// test pairs
	var err error
	var sel fdb.KeyValue
	key1 := fdb.Key("key1")
	key2 := fdb.Key("key2")
	val1 := []byte("val1")
	val2 := []byte("val2")

	// insert and check val1
	if err = s.tx.Upsert([]fdb.KeyValue{{key1, val1}}); s.NoError(err) {
		if sel, err = s.tx.Select(key1); s.NoError(err) {
			s.Equal(string(val1), string(sel.Value))
		}
	}

	// check there are no key2
	if _, err = s.tx.Select(key2); s.Error(err) {
		s.True(errx.Is(err, mvcc.ErrSelect))
		s.True(errx.Is(err, mvcc.ErrNotFound))
	}

	onInsert := func(tx mvcc.Tx, w db.Writer, p fdb.KeyValue) error {
		s.NotNil(p)
		s.Equal(val2, p.Value)
		s.Equal(key2.String(), p.Key.String())
		return nil
	}

	// insert and check val2
	if err = s.tx.Upsert([]fdb.KeyValue{{key2, val2}}, mvcc.OnInsert(onInsert)); s.NoError(err) {
		if sel, err = s.tx.Select(key2); s.NoError(err) {
			s.Equal(string(val2), string(sel.Value))
		}
	}

	onDelete := func(tx mvcc.Tx, w db.Writer, p fdb.KeyValue) error {
		s.Equal(key1.String(), p.Key.String())
		return nil
	}

	// delete and check there are no val1
	if err = s.tx.Delete([]fdb.Key{key1}, mvcc.OnDelete(onDelete)); s.NoError(err) {
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
			pair := fdb.KeyValue{fdb.Key(fmt.Sprintf("key%d", i%9)), []byte(strconv.Itoa(i))}
			s.Require().NoError(s.tx.Upsert([]fdb.KeyValue{pair}))
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
			tx := mvcc.Begin(s.cn)
			pair := fdb.KeyValue{fdb.Key(fmt.Sprintf("key%d", i%9)), []byte(strconv.Itoa(i))}
			s.Require().NoError(tx.Upsert([]fdb.KeyValue{pair}))

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
	tx := mvcc.Begin(s.cn)

	tx.OnCommit(func(w db.Writer) error {
		return mvcc.ErrBLOBDrop.WithStack()
	})

	if err := tx.Commit(); s.Error(err) {
		s.True(errx.Is(err, mvcc.ErrBLOBDrop, mvcc.ErrClose))
	}
	s.Require().NoError(tx.Cancel())

	tx = mvcc.Begin(s.cn)

	var num int64 = 123
	var key = fdb.Key("key")

	tx.OnCommit(func(w db.Writer) error {
		w.Increment(key, num)
		return nil
	})

	s.Require().NoError(s.cn.Write(func(w db.Writer) error {
		s.Require().NoError(tx.Commit(mvcc.Writer(w)))
		s.Equal(num, int64(binary.LittleEndian.Uint64(w.Data(key))))
		return nil
	}))
}

func (s *MVCCSuite) TestBLOB() {
	key := fdb.Key("test blob")

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
	key1 := fdb.Key("key1")
	key2 := fdb.Key("key2")
	key3 := fdb.Key("key3")
	key4 := fdb.Key("key4")
	key5 := fdb.Key("key5")
	key6 := fdb.Key("key6")
	key7 := fdb.Key("key7")
	val1 := []byte("val1")
	val2 := []byte("val2")
	val3 := []byte("val3")
	val4 := []byte("val4")
	val5 := []byte("val5")
	val6 := []byte("val6")
	val7 := []byte("val7")

	s.Require().NoError(s.tx.Upsert([]fdb.KeyValue{
		fdb.KeyValue{key1, val1},
		fdb.KeyValue{key2, val2},
		fdb.KeyValue{key3, val3},
		fdb.KeyValue{key4, val4},
		fdb.KeyValue{key5, val5},
		fdb.KeyValue{key6, val6},
		fdb.KeyValue{key7, val7},
	}))

	ctx := context.Background()

	if list, err := s.tx.ListAll(ctx); s.NoError(err) {
		s.Len(list, 7)
	}

	if list, err := s.tx.ListAll(ctx, mvcc.From(key2), mvcc.Last(key6)); s.NoError(err) {
		s.Len(list, 5)
	}
	s.Require().NoError(s.tx.Commit())

	// Удаляем парочку и коммитим это

	tx := mvcc.Begin(s.cn)
	s.Require().NoError(tx.Delete([]fdb.Key{key3, key5}))
	s.Require().NoError(tx.Commit())

	// Удаляем еще парочку, но не коммитим

	tx = mvcc.Begin(s.cn)
	s.Require().NoError(tx.Delete([]fdb.Key{key4, key6}))

	// В отдельной транзакции запрашиваем результат

	vals := make([]string, 0, 2)
	hdlr := func(tx mvcc.Tx, w db.Writer, p fdb.KeyValue) error {
		vals = append(vals, string(p.Value))
		return nil
	}

	tx2 := mvcc.Begin(s.cn)

	s.Require().NoError(s.cn.Write(func(w db.Writer) error {
		if list, err := tx2.ListAll(
			ctx,
			mvcc.From(key2),
			mvcc.Limit(3),
			mvcc.Writer(w),
			mvcc.Exclusive(hdlr),
		); s.NoError(err) {
			s.Len(list, 3)
			s.Equal([]string{"val2", "val4", "val6"}, vals)
		}

		vals = make([]string, 0, 2)
		if list, err := tx2.ListAll(
			ctx,
			mvcc.From(key2),
			mvcc.Limit(3),
			mvcc.Writer(w),
			mvcc.Exclusive(hdlr),
			mvcc.Reverse(),
		); s.NoError(err) {
			s.Len(list, 3)
			s.Equal([]string{"val7", "val6", "val4"}, vals)
		}

		return nil
	}))
	s.Require().NoError(tx.Cancel())
	s.Require().NoError(tx2.Cancel())
}

func (s *MVCCSuite) TestSharedLock() {
	key := fdb.Key("key")
	lock := fdb.Key("lock")

	wg := new(sync.WaitGroup)
	wg.Add(2)

	go func() {
		defer wg.Done()

		tx := mvcc.Begin(s.cn)
		defer tx.Cancel()

		// В первой транзакции забираем блокировку
		s.Require().NoError(tx.SharedLock(lock, time.Second))

		// Ждем минутку, типа чот делаем
		time.Sleep(100 * time.Millisecond)

		// Пытаемся получить блокировку повторно, это должно работать
		s.Require().NoError(tx.SharedLock(lock, time.Second))

		// Обновляем значение
		s.Require().NoError(tx.Upsert([]fdb.KeyValue{{key, []byte("val1")}}))
		s.Require().NoError(tx.Commit())
	}()

	go func() {
		defer wg.Done()

		tx := mvcc.Begin(s.cn)
		defer tx.Cancel()

		// Во второй транзакции сначала чуть ждем, чтобы первая точно успела взять блокировку
		time.Sleep(20 * time.Millisecond)

		// Пытаемся получить блокировку
		s.Require().NoError(tx.SharedLock(lock, time.Second))

		// Теперь читаем значение. Если блокировка работает, то будет ждать освобождения и прочитаем val1
		// Если не работает, то прочитает сразу же, там будет пустое значение
		if sel, err := tx.Select(key); s.NoError(err) {
			s.Equal("val1", string(sel.Value))
		}

		// Специально не коммитим, чтобы проверить снятие блокировки по отмене
	}()

	wg.Wait()

	// После двух транзакций не должно быть никакой проблемы с блокировкой, т.к. она должна быть снята
	tx := mvcc.Begin(s.cn)
	defer tx.Cancel()
	s.Require().NoError(tx.SharedLock(lock, time.Second))
}

func BenchmarkSequenceWorkflowDiffTx(b *testing.B) {
	cn, err := db.Connect(TestDB)
	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	b.ResetTimer()

	var keys [1]fdb.Key
	var pairs [1]fdb.KeyValue

	for i := 0; i < b.N; i++ {
		key := fdb.Key(typex.NewUUID())
		val := []byte(typex.NewUUID().String())
		val2 := []byte(typex.NewUUID().String())

		// INSERT
		tx := mvcc.Begin(cn)
		pairs[0] = fdb.KeyValue{key, val}
		require.NoError(b, tx.Upsert(pairs[:]))
		require.NoError(b, tx.Commit())

		// UPDATE
		tx = mvcc.Begin(cn)
		pairs[0] = fdb.KeyValue{key, val2}
		require.NoError(b, tx.Upsert(pairs[:]))
		require.NoError(b, tx.Commit())

		// SELECT / DELETE
		tx = mvcc.Begin(cn)
		sl, err := tx.Select(key)
		require.NoError(b, err)
		require.Equal(b, val2, sl.Value)
		keys[0] = key
		require.NoError(b, tx.Delete(keys[:]))
		require.NoError(b, tx.Commit())

		// SELECT EMPTY
		tx = mvcc.Begin(cn)
		sl, err = tx.Select(key)
		require.Error(b, err)
		require.True(b, errx.Is(err, mvcc.ErrNotFound))
		require.NoError(b, tx.Cancel())
	}
}

func BenchmarkSequenceWorkflowSameTx(b *testing.B) {
	cn, err := db.Connect(TestDB)
	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fdb.Key(typex.NewUUID())
		val := []byte(typex.NewUUID().String())
		val2 := []byte(typex.NewUUID().String())

		// INSERT
		tx := mvcc.Begin(cn)
		require.NoError(b, tx.Upsert([]fdb.KeyValue{{key, val}}))

		// UPDATE
		require.NoError(b, tx.Upsert([]fdb.KeyValue{{key, val2}}))

		// SELECT / DELETE
		sl, err := tx.Select(key)
		require.NoError(b, err)
		require.Equal(b, string(val2), string(sl.Value))
		require.NoError(b, tx.Delete([]fdb.Key{key}))

		// SELECT EMPTY
		sl, err = tx.Select(key)
		require.Error(b, err)
		require.True(b, errx.Is(err, mvcc.ErrNotFound))
		require.NoError(b, tx.Commit())
	}
}

func BenchmarkOperationsSameTx(b *testing.B) {
	cn, err := db.Connect(TestDB)
	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	uid := []byte(typex.NewUUID())
	key := fdb.Key(uid)
	val := uid

	tx := mvcc.Begin(cn)
	require.NoError(b, tx.Upsert([]fdb.KeyValue{{key, uid}}))

	b.ResetTimer()

	b.Run("UPSERT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			uid := []byte(typex.NewUUID())
			require.NoError(b, tx.Upsert([]fdb.KeyValue{{uid, uid}}))
		}
	})

	b.Run("SELECT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sl, err := tx.Select(key)
			require.NoError(b, err)
			require.Equal(b, string(val), string(sl.Value))
		}
	})

	b.Run("DELETE", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			uid := []byte(typex.NewUUID())
			require.NoError(b, tx.Upsert([]fdb.KeyValue{{uid, uid}}))
			require.NoError(b, tx.Delete([]fdb.Key{uid}))
		}
	})
}
