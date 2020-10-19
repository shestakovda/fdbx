package orm_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/fdbx/v2/orm"
	"github.com/shestakovda/typex"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const TestDB byte = 0x10
const TestTable uint16 = 1
const TestIndex byte = 1
const TestQueue byte = 2

func TestORM(t *testing.T) {
	suite.Run(t, new(ORMSuite))
}

type ORMSuite struct {
	suite.Suite

	tx  mvcc.Tx
	cn  db.Connection
	tbl orm.Collection
}

func (s *ORMSuite) SetupTest() {
	var err error

	s.cn, err = db.ConnectV610(TestDB)
	s.Require().NoError(err)
	s.Require().NoError(s.cn.Clear())

	s.tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)

	s.tbl = orm.Table(
		TestTable,
		orm.Index(TestIndex, func(v []byte) fdbx.Key {
			if len(v) > 3 {
				return fdbx.Key(v[:3])
			}
			return nil
		}),
	)
}

func (s *ORMSuite) TearDownTest() {
	s.NoError(s.tx.Cancel())
}

func (s *ORMSuite) TestWorkflow() {
	var key fdbx.Key
	var val []byte

	// Чтобы потестить сжатие в gzip
	baseMsg := "message"

	parts := make([]string, 0, 200)
	for {
		parts = append(parts, typex.NewUUID().String())
		if len(parts) > 200 { // около 7 кб
			break
		}
	}
	longMsg := strings.Join(parts, "")

	parts = make([]string, 0, 200000)
	for {
		parts = append(parts, typex.NewUUID().String())
		if len(parts) > 200000 { // около 7 кб
			break
		}
	}
	hugeMsg := strings.Join(parts, "") // около 7 мб

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdbx.NewPair(fdbx.Key("id1"), []byte(baseMsg)),
		fdbx.NewPair(fdbx.Key("id2"), []byte(longMsg)),
		fdbx.NewPair(fdbx.Key("id3"), []byte(hugeMsg)),
	))

	if list, err := s.tbl.Select(s.tx).All(); s.NoError(err) {
		s.Len(list, 3)

		if key, err = list[0].Key(); s.NoError(err) {
			s.Equal("id1", key.String())
		}
		if val, err = list[0].Value(); s.NoError(err) {
			s.Equal(baseMsg, string(val))
		}

		if key, err = list[1].Key(); s.NoError(err) {
			s.Equal("id2", key.String())
		}
		if val, err = list[1].Value(); s.NoError(err) {
			s.Equal(longMsg, string(val))
		}

		if key, err = list[2].Key(); s.NoError(err) {
			s.Equal("id3", key.String())
		}
		if val, err = list[2].Value(); s.NoError(err) {
			s.Equal(hugeMsg, string(val))
		}
	}

	s.Require().NoError(s.tbl.Select(s.tx).Delete())

	if list, err := s.tbl.Select(s.tx).All(); s.NoError(err) {
		s.Empty(list)
	}
}

func (s *ORMSuite) TestCount() {
	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdbx.NewPair(fdbx.Key("id1"), []byte("msg1")),
		fdbx.NewPair(fdbx.Key("id2"), []byte("msg2")),
		fdbx.NewPair(fdbx.Key("id3"), []byte("msg3")),
	))

	var cnt uint64
	cnt2 := uint64(3)

	if err := s.tbl.Select(s.tx).Agg(orm.Count(&cnt), orm.Count(&cnt2)); s.NoError(err) {
		s.Equal(uint64(3), cnt)
		s.Equal(uint64(6), cnt2)
	}
}

func (s *ORMSuite) TestByID() {
	var key fdbx.Key
	var val []byte

	id1 := fdbx.Key("id1")
	id2 := fdbx.Key("id2")
	id3 := fdbx.Key("id3")
	id4 := fdbx.Key("id4")

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdbx.NewPair(id1, []byte("msg1")),
		fdbx.NewPair(id2, []byte("msg2")),
		fdbx.NewPair(id3, []byte("msg3")),
	))

	if list, err := s.tbl.Select(s.tx).ByID(id1, id3).All(); s.NoError(err) {
		s.Len(list, 2)

		if key, err = list[0].Key(); s.NoError(err) {
			s.Equal(id1.String(), key.String())
		}
		if val, err = list[0].Value(); s.NoError(err) {
			s.Equal("msg1", string(val))
		}

		if key, err = list[1].Key(); s.NoError(err) {
			s.Equal(id3.String(), key.String())
		}
		if val, err = list[1].Value(); s.NoError(err) {
			s.Equal("msg3", string(val))
		}
	}

	if list, err := s.tbl.Select(s.tx).ByID(id2, id4).All(); s.Error(err) {
		s.Nil(list)
		s.True(errx.Is(err, orm.ErrSelect))
		s.True(errx.Is(err, orm.ErrNotFound))
	}

	if list, err := s.tbl.Select(s.tx).PossibleByID(id2, id4).All(); s.NoError(err) {
		s.Len(list, 1)

		if key, err = list[0].Key(); s.NoError(err) {
			s.Equal(id2.String(), key.String())
		}
		if val, err = list[0].Value(); s.NoError(err) {
			s.Equal("msg2", string(val))
		}
	}

	if pair, err := s.tbl.Select(s.tx).PossibleByID(id2, id3).First(); s.NoError(err) {
		if key, err = pair.Key(); s.NoError(err) {
			s.Equal(id2.String(), key.String())
		}
		if val, err = pair.Value(); s.NoError(err) {
			s.Equal("msg2", string(val))
		}
	}
}

func (s *ORMSuite) TestQueue() {
	const recCount = 3

	id1 := fdbx.Key("id1")
	id2 := fdbx.Key("id2")
	id3 := fdbx.Key("id3")
	id4 := fdbx.Key("id4")

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdbx.NewPair(id1, []byte("msg1")),
		fdbx.NewPair(id2, []byte("msg2")),
		fdbx.NewPair(id3, []byte("msg3")),
	))
	s.Require().NoError(s.tx.Commit())

	q := orm.Queue(TestQueue, s.tbl, orm.PunchTime(10*time.Millisecond))

	var wg sync.WaitGroup
	wg.Add(1)

	// subscribe
	go func() {
		defer wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		recc, errc := q.Sub(ctx, s.cn, 1)

		recs := make([]fdbx.Pair, 0, recCount)
		for rec := range recc {
			recs = append(recs, rec)
		}

		errs := make([]error, 0, 1)
		for err := range errc {
			errs = append(errs, err)
		}

		s.Len(recs, recCount)
		s.Len(errs, 1)
		s.True(errors.Is(errs[0], context.DeadlineExceeded))
	}()

	// Публикация задачи, которой нет
	tx, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)
	s.Require().NoError(q.Pub(tx, time.Now().Add(5*time.Millisecond), id4))
	s.Require().NoError(tx.Commit())

	// Публикация задач по очереди
	tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)
	s.Require().NoError(q.Pub(tx, time.Now().Add(time.Millisecond), id1))
	s.Require().NoError(tx.Commit())

	tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)
	s.Require().NoError(q.Pub(tx, time.Now().Add(50*time.Millisecond), id2))
	s.Require().NoError(tx.Commit())

	tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)
	s.Require().NoError(q.Pub(tx, time.Now().Add(200*time.Millisecond), id3))
	s.Require().NoError(tx.Commit())

	// Ждем завершения подписки
	wg.Wait()

	// Проверяем стату
	tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)

	if lost, err := q.Lost(tx, 100); s.NoError(err) {
		s.Len(lost, 3)
	}

	if wait, work, err := q.Stat(tx); s.NoError(err) {
		s.Equal(int64(0), wait)
		s.Equal(int64(3), work)
	}

	if err := q.Ack(tx, id2); s.NoError(err) {
		if stat, err := q.Status(tx, id1, id2, id3); s.NoError(err) {
			s.Equal(map[string]byte{
				"id1": orm.StatusUnconfirmed,
				"id2": orm.StatusConfirmed,
				"id3": orm.StatusUnconfirmed,
			}, stat)
		}

		if lost, err := q.Lost(tx, 100); s.NoError(err) {
			s.Len(lost, 2)
		}
	}

	if err := q.Ack(tx, id1, id3); s.NoError(err) {
		if stat, err := q.Status(tx, id1, id2, id3); s.NoError(err) {
			s.Equal(map[string]byte{
				"id1": orm.StatusConfirmed,
				"id2": orm.StatusConfirmed,
				"id3": orm.StatusConfirmed,
			}, stat)
		}

		if lost, err := q.Lost(tx, 100); s.NoError(err) {
			s.Len(lost, 0)
		}
	}
}

func BenchmarkUpsert(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx, err := mvcc.Begin(cn)
	require.NoError(b, err)
	defer tx.Cancel()

	tbl := orm.Table(TestTable)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		uid := []byte(typex.NewUUID())
		require.NoError(b, tbl.Upsert(tx, fdbx.NewPair(uid, uid)))
	}
}

func BenchmarkUpsertBatch(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx, err := mvcc.Begin(cn)
	require.NoError(b, err)
	defer tx.Cancel()

	tbl := orm.Table(TestTable)

	count := 1000
	batch := make([]fdbx.Pair, count)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for i := range batch {
			uid := []byte(typex.NewUUID())
			batch[i] = fdbx.NewPair(uid, uid)
		}
		require.NoError(b, tbl.Upsert(tx, batch...))
	}
}

func BenchmarkCount(b *testing.B) {
	// const count = 1000000
	// batchSize := 10000
	// mvcc.ScanRangeSize = 100000

	const count = 10000
	batchSize := 10000

	cn, err := db.ConnectV610(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx, err := mvcc.Begin(cn)
	require.NoError(b, err)

	tbl := orm.Table(TestTable)

	for k := 0; k < count/int(batchSize); k++ {
		batch := make([]fdbx.Pair, batchSize)
		for i := 0; i < int(batchSize); i++ {
			uid := []byte(typex.NewUUID())
			batch[i] = fdbx.NewPair(uid, uid)
		}
		require.NoError(b, tbl.Upsert(tx, batch...))
	}

	require.NoError(b, tx.Commit())

	tx, err = mvcc.Begin(cn)
	require.NoError(b, err)
	defer tx.Cancel()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cnt := uint64(0)
		require.NoError(b, tbl.Select(tx).Agg(orm.Count(&cnt)))
		require.Equal(b, count, int(cnt))
	}

}
