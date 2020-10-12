package orm_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/fdbx/v2/orm"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const TestDB byte = 0x10
const TestTable uint16 = 1

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

	s.tbl = orm.Table(TestTable)
}

func (s *ORMSuite) TearDownTest() {
	s.NoError(s.tx.Cancel())
}

func (s *ORMSuite) TestWorkflow() {
	var key fdbx.Key
	var val fdbx.Value

	// Чтобы потестить сжатие в gzip
	longMsg := strings.Repeat("msg3", 300)

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdbx.NewPair(fdbx.Key("id1"), fdbx.Value("msg1")),
		fdbx.NewPair(fdbx.Key("id2"), fdbx.Value("msg2")),
		fdbx.NewPair(fdbx.Key("id3"), fdbx.Value(longMsg)),
	))

	if list, err := s.tbl.Select(s.tx).All(); s.NoError(err) {
		s.Len(list, 3)

		if key, err = list[0].Key(); s.NoError(err) {
			s.Equal("id1", key.String())
		}
		if val, err = list[0].Value(); s.NoError(err) {
			s.Equal("msg1", val.String())
		}

		if key, err = list[1].Key(); s.NoError(err) {
			s.Equal("id2", key.String())
		}
		if val, err = list[1].Value(); s.NoError(err) {
			s.Equal("msg2", val.String())
		}

		if key, err = list[2].Key(); s.NoError(err) {
			s.Equal("id3", key.String())
		}
		if val, err = list[2].Value(); s.NoError(err) {
			s.Equal(longMsg, val.String())
		}
	}

	s.Require().NoError(s.tbl.Select(s.tx).Delete())

	if list, err := s.tbl.Select(s.tx).All(); s.NoError(err) {
		s.Empty(list)
	}
}

func (s *ORMSuite) TestCount() {
	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdbx.NewPair(fdbx.Key("id1"), fdbx.Value("msg1")),
		fdbx.NewPair(fdbx.Key("id2"), fdbx.Value("msg2")),
		fdbx.NewPair(fdbx.Key("id3"), fdbx.Value("msg3")),
	))

	var cnt uint64
	cnt2 := uint64(3)

	if err := s.tbl.Select(s.tx).Agg(orm.Count(&cnt), orm.Count(&cnt2)); s.NoError(err) {
		s.Equal(uint64(3), cnt)
		s.Equal(uint64(6), cnt2)
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
		uid := uuid.New()
		require.NoError(b, tbl.Upsert(tx, fdbx.NewPair(uid[:], uid[:])))
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
			uid := uuid.New()
			batch[i] = fdbx.NewPair(uid[:], uid[:])
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
	mvcc.ScanRangeSize = 1000

	cn, err := db.ConnectV610(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx, err := mvcc.Begin(cn)
	require.NoError(b, err)

	tbl := orm.Table(TestTable)

	for k := 0; k < count/int(batchSize); k++ {
		batch := make([]fdbx.Pair, batchSize)
		for i := 0; i < int(batchSize); i++ {
			uid := uuid.New()
			batch[i] = fdbx.NewPair(uid[:], uid[:])
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
