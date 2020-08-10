package orm_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shestakovda/fdbx/db"
	"github.com/shestakovda/fdbx/mvcc"
	"github.com/shestakovda/fdbx/orm"
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

	s.tbl = orm.Table(TestTable, newModel)
}

func (s *ORMSuite) TearDownTest() {
	s.NoError(s.tx.Cancel())
}

func (s *ORMSuite) TestWorkflow() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	s.Require().NoError(s.tbl.Upsert(s.tx,
		&simpleModel{ID: "id1", Msg: "msg1"},
		&simpleModel{ID: "id2", Msg: "msg2"},
		&simpleModel{ID: "id3", Msg: "msg3"},
	))

	if list, err := s.tbl.Select(s.tx).All(ctx); s.NoError(err) {
		s.Len(list, 3)

		s.Equal("id1", list[0].(*simpleModel).ID)
		s.Equal("msg1", list[0].(*simpleModel).Msg)

		s.Equal("id2", list[1].(*simpleModel).ID)
		s.Equal("msg2", list[1].(*simpleModel).Msg)

		s.Equal("id3", list[2].(*simpleModel).ID)
		s.Equal("msg3", list[2].(*simpleModel).Msg)
	}

	s.Require().NoError(s.tbl.Select(s.tx).Delete(ctx))

	if list, err := s.tbl.Select(s.tx).All(ctx); s.NoError(err) {
		s.Empty(list)
	}
}

func (s *ORMSuite) TestCount() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	s.Require().NoError(s.tbl.Upsert(s.tx,
		&simpleModel{ID: "id1", Msg: "msg1"},
		&simpleModel{ID: "id2", Msg: "msg2"},
		&simpleModel{ID: "id3", Msg: "msg3"},
	))

	var cnt uint64
	cnt2 := uint64(3)

	if err := s.tbl.Select(s.tx).Agg(ctx, orm.Count(&cnt), orm.Count(&cnt2)); s.NoError(err) {
		s.Equal(uint64(3), cnt)
		s.Equal(uint64(6), cnt2)
	}
}

func newModel(key mvcc.Key) orm.Model {
	return &simpleModel{
		ID: key.String(),
	}
}

type simpleModel struct {
	ID  string
	Msg string
}

func (m simpleModel) Key() mvcc.Key              { return mvcc.NewStrKey(m.ID) }
func (m simpleModel) Pack() (mvcc.Value, error)  { return mvcc.NewStrValue(m.Msg), nil }
func (m *simpleModel) Unpack(v mvcc.Value) error { m.Msg = v.String(); return nil }

func BenchmarkUpsert(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx, err := mvcc.Begin(cn)
	require.NoError(b, err)
	defer tx.Cancel()

	tbl := orm.Table(TestTable, newModel)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := uuid.New().String()
		require.NoError(b, tbl.Upsert(tx, &simpleModel{ID: key, Msg: key}))
	}
}

func BenchmarkUpsertBatch(b *testing.B) {
	cn, err := db.ConnectV610(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx, err := mvcc.Begin(cn)
	require.NoError(b, err)
	defer tx.Cancel()

	tbl := orm.Table(TestTable, newModel)

	count := 10000
	batch := make([]orm.Model, count)

	for i := range batch {
		key := uuid.New().String()
		batch[i] = &simpleModel{ID: key, Msg: key}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		require.NoError(b, tbl.Upsert(tx, batch...))
	}
}

func BenchmarkCount(b *testing.B) {
	const count = 1000000

	mvcc.ScanRangeSize = 10000

	cn, err := db.ConnectV610(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx, err := mvcc.Begin(cn)
	require.NoError(b, err)

	tbl := orm.Table(TestTable, newModel)

	for k := 0; k < count/mvcc.ScanRangeSize; k++ {
		batch := make([]orm.Model, mvcc.ScanRangeSize)
		for i := 0; i < mvcc.ScanRangeSize; i++ {
			key := uuid.New().String()
			batch[i] = &simpleModel{ID: key, Msg: key}
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
		require.NoError(b, tbl.Select(tx).Agg(context.Background(), orm.Count(&cnt)))
		require.Equal(b, uint64(count), cnt)
	}

}
