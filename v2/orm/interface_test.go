package orm_test

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/golang/glog"
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/fdbx/v2/orm"
	"github.com/shestakovda/typex"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const TestDB byte = 0x10
const TestTable uint16 = 0xAAAA
const TestIndex uint16 = 0xBBBB
const TestQueue uint16 = 0xCCCC
const TestIndex2 uint16 = 0xDDDD

func TestORM(t *testing.T) {
	suite.Run(t, new(ORMSuite))
}

type ORMSuite struct {
	suite.Suite

	tx  mvcc.Tx
	cn  db.Connection
	tbl orm.Table
	idx []string
}

func (s *ORMSuite) SetupTest() {
	var err error

	s.cn, err = db.Connect(TestDB)
	s.Require().NoError(err)
	s.Require().NoError(s.cn.Clear())

	s.tx = mvcc.Begin(s.cn)

	s.idx = make([]string, 0, 8)
	s.tbl = orm.NewTable(
		TestTable,
		orm.Index(TestIndex, func(v []byte) (k fdb.Key, e error) {
			if len(v) < 4 {
				return nil, nil
			}

			k = v[:4]
			s.idx = append(s.idx, k.String())
			return k, nil
		}),
		orm.MultiIndex(TestIndex2, func(v []byte) ([]fdb.Key, error) {
			if len(v) < 4 {
				return nil, nil
			}

			return []fdb.Key{v[0:2], v[2:4]}, nil
		}),
	)
}

func (s *ORMSuite) TearDownTest() {
	s.NoError(s.tx.Cancel())
}

func (s *ORMSuite) checkVacuum(ignore map[string]bool) {
	s.Require().NoError(s.tbl.Vacuum(s.cn))

	// В базе ничего не должно оставаться
	s.Require().NoError(s.cn.Read(func(r db.Reader) error {
		fail := false
		nkey := fdb.Key{0x00}
		list := r.List(nkey, nkey, 1000, false, false).GetSliceOrPanic()

		for i := range list {
			if pkey := list[i].Key[1:].String(); !ignore[pkey] {
				glog.Errorf(">> %s", pkey)
				fail = true
			}
		}

		s.False(fail)
		return nil
	}))
}

func (s *ORMSuite) TestWorkflow() {
	// Обычное значение
	baseMsg := []byte("msg")

	// Чтобы потестить сжатие в gzip
	longMsg := make([]byte, 20<<10)
	_, err := rand.Read(longMsg)
	s.Require().NoError(err)

	// Чтобы потестить BLOB
	hugeMsg := make([]byte, 20<<20)
	_, err = rand.Read(hugeMsg)
	s.Require().NoError(err)

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdb.KeyValue{fdb.Key("id1"), baseMsg},
		fdb.KeyValue{fdb.Key("id2"), longMsg},
		fdb.KeyValue{fdb.Key("id3"), hugeMsg},
	))

	if list, err := s.tbl.Select(s.tx).All(); s.NoError(err) {
		s.Len(list, 3)

		s.Equal("id1", list[0].Key.String())
		s.Equal(baseMsg, list[0].Value)

		s.Equal("id2", list[1].Key.String())
		s.Equal(longMsg, list[1].Value)

		s.Equal("id3", list[2].Key.String())
		s.Equal(hugeMsg, list[2].Value)
	}

	s.Require().NoError(s.tbl.Select(s.tx).Delete())

	if list, err := s.tbl.Select(s.tx).All(); s.NoError(err) {
		s.Empty(list)
	}

	// Удаляем строки, чтобы автовакуум их собрал
	s.Require().NoError(s.tx.Commit())

	s.checkVacuum(nil)
}

func (s *ORMSuite) TestCount() {
	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdb.KeyValue{fdb.Key("id1"), []byte("msg1")},
		fdb.KeyValue{fdb.Key("id2"), []byte("msg2")},
		fdb.KeyValue{fdb.Key("id3"), []byte("msg3")},
	))

	var cnt uint64
	cnt2 := uint64(3)

	if err := s.tbl.Select(s.tx).Agg(orm.Count(&cnt), orm.Count(&cnt2)); s.NoError(err) {
		s.Equal(3, int(cnt))
		s.Equal(6, int(cnt2))
	}
}

func (s *ORMSuite) TestByID() {
	id1 := fdb.Key("id1")
	id2 := fdb.Key("id2")
	id3 := fdb.Key("id3")
	id4 := fdb.Key("id4")

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdb.KeyValue{id1, []byte("msg1")},
		fdb.KeyValue{id2, []byte("msg2")},
		fdb.KeyValue{id3, []byte("msg3")},
	))

	if err := s.tbl.Insert(s.tx, fdb.KeyValue{id2, []byte("msg4")}); s.Error(err) {
		s.True(errx.Is(err, orm.ErrDuplicate))
	}

	if list, err := s.tbl.Select(s.tx).ByID(id1, id3).All(); s.NoError(err) {
		s.Len(list, 2)

		s.Equal(id1.String(), list[0].Key.String())
		s.Equal("msg1", string(list[0].Value))

		s.Equal(id3.String(), list[1].Key.String())
		s.Equal("msg3", string(list[1].Value))
	}

	if list, err := s.tbl.Select(s.tx).ByID(id2, id4).All(); s.Error(err) {
		s.Nil(list)
		s.True(errx.Is(err, orm.ErrSelect))
		s.True(errx.Is(err, orm.ErrNotFound))
	}

	if list, err := s.tbl.Select(s.tx).PossibleByID(id4, id2).All(); s.NoError(err) {
		s.Len(list, 1)

		s.Equal(id2.String(), list[0].Key.String())
		s.Equal("msg2", string(list[0].Value))
	}

	if pair, err := s.tbl.Select(s.tx).PossibleByID(id4, id2, id3).First(); s.NoError(err) {
		s.Equal(id2.String(), pair.Key.String())
		s.Equal("msg2", string(pair.Value))
	}

	// Удаляем строки, чтобы автовакуум их собрал
	s.Require().NoError(s.tbl.Select(s.tx).Delete())
	s.Require().NoError(s.tx.Commit())

	s.checkVacuum(nil)
}

func (s *ORMSuite) TestUndo() {
	id1 := fdb.Key("id1")
	id2 := fdb.Key("id2")
	id3 := fdb.Key("id3")

	q := orm.NewQueue(TestQueue, s.tbl, orm.Refresh(10*time.Millisecond), orm.Prefix([]byte("lox")))

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdb.KeyValue{id1, []byte("msg1")},
		fdb.KeyValue{id2, []byte("msg2")},
		fdb.KeyValue{id3, []byte("msg3")},
	))
	s.Require().NoError(s.tx.Commit())

	// Публикация задач по очереди (и нарочно одну и ту же несколько раз)
	tx := mvcc.Begin(s.cn)
	s.Require().NoError(q.PubList(tx, []fdb.Key{id1, id1, id2, id2, id3}, orm.Delay(time.Millisecond)))
	s.Require().NoError(tx.Commit())

	// Теперь делаем отмену для пары задач
	tx = mvcc.Begin(s.cn)
	s.Require().NoError(q.Undo(tx, id1))
	s.Require().NoError(q.Undo(tx, id3))
	s.Require().NoError(tx.Commit())

	// Проверяем стату
	tx = mvcc.Begin(s.cn)

	// Должно быть 2 потеряшки (которых мы отменили)
	if wait, work, err := q.Stat(tx); s.NoError(err) {
		s.Equal(int64(1), wait) // id2
		s.Equal(int64(2), work) // id1 id3
	}
	if lost, err := q.Lost(tx, 100); s.NoError(err) && s.Len(lost, 2) {
		s.Equal(id1, lost[0].Key())
		s.Equal(id3, lost[1].Key())
	}
	s.Require().NoError(tx.Commit())
}

func (s *ORMSuite) TestQueue() {
	id1 := fdb.Key("id1")
	id2 := fdb.Key("id2")
	id3 := fdb.Key("id3")
	id4 := fdb.Key("id4")

	q := orm.NewQueue(TestQueue, s.tbl, orm.Refresh(10*time.Millisecond), orm.Prefix([]byte("lox")))

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdb.KeyValue{id1, []byte("msg1")},
		fdb.KeyValue{id2, []byte("msg2")},
		fdb.KeyValue{id3, []byte("msg3")},
	))
	if wait, work, err := q.Stat(s.tx); s.NoError(err) {
		s.Equal(int64(0), wait)
		s.Equal(int64(0), work)
	}
	s.Require().NoError(s.tx.Commit())

	var wg sync.WaitGroup
	wg.Add(1)

	// subscribe
	go func() {
		defer wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		recc, errc := q.Sub(ctx, s.cn, 1)

		recs := make([]orm.Task, 0, 2)
		for rec := range recc {
			recs = append(recs, rec)
		}

		errs := make([]error, 0, 1)
		for err := range errc {
			errs = append(errs, err)
		}

		s.Len(recs, 2)
		s.Len(errs, 1)
		if !s.True(errors.Is(errs[0], context.DeadlineExceeded)) {
			s.NoError(errs[0])
		}
	}()

	// Публикация задачи, которой нет
	tx := mvcc.Begin(s.cn)
	s.Require().NoError(q.Pub(tx, id4, orm.Delay(5*time.Millisecond)))
	s.Require().NoError(tx.Commit())

	// Публикация задач по очереди (и нарочно одну и ту же два раза)
	tx = mvcc.Begin(s.cn)
	s.Require().NoError(q.PubList(tx, []fdb.Key{id1, id2, id2}, orm.Delay(time.Millisecond)))
	s.Require().NoError(tx.Commit())

	tx = mvcc.Begin(s.cn)
	s.Require().NoError(q.Pub(tx, id3, orm.Delay(time.Hour)))
	s.Require().NoError(tx.Commit())

	// Ждем завершения подписки
	wg.Wait()

	// Проверяем стату
	tx = mvcc.Begin(s.cn)

	if lost, err := q.Lost(tx, 100); s.NoError(err) {
		s.Len(lost, 2)
	}

	if wait, work, err := q.Stat(tx); s.NoError(err) {
		s.Equal(int64(1), wait) // id3
		s.Equal(int64(2), work) // id1 id2
	}

	if err := q.Ack(tx, id2); s.NoError(err) {
		if task, err := q.Task(tx, id1); s.NoError(err) {
			s.Equal(orm.StatusUnconfirmed, task.Status())
		}

		if task, err := q.Task(tx, id2); s.NoError(err) {
			s.Equal(orm.StatusConfirmed, task.Status())
		}

		if task, err := q.Task(tx, id3); s.NoError(err) {
			s.Equal(orm.StatusPublished, task.Status())
		}

		if task, err := q.Task(tx, id4); s.NoError(err) {
			s.Equal(orm.StatusConfirmed, task.Status())
		}

		if lost, err := q.Lost(tx, 100); s.NoError(err) {
			s.Len(lost, 1)
		}
	}

	if err := q.Ack(tx, id1, id3, id4); s.NoError(err) {
		if task, err := q.Task(tx, id1); s.NoError(err) {
			s.Equal(orm.StatusConfirmed, task.Status())
		}

		if task, err := q.Task(tx, id2); s.NoError(err) {
			s.Equal(orm.StatusConfirmed, task.Status())
		}

		if task, err := q.Task(tx, id3); s.NoError(err) {
			s.Equal(orm.StatusPublished, task.Status())
		}

		if task, err := q.Task(tx, id4); s.NoError(err) {
			s.Equal(orm.StatusConfirmed, task.Status())
		}

		if lost, err := q.Lost(tx, 100); s.NoError(err) {
			s.Len(lost, 0)
		}
	}

	if wait, work, err := q.Stat(tx); s.NoError(err) {
		s.Equal(int64(1), wait) // id3
		s.Equal(int64(0), work) // id1 id2
	}

	if err := q.Undo(tx, id3); s.NoError(err) {
		if lost, err := q.Lost(tx, 100); s.NoError(err) {
			s.Len(lost, 1) // id3
		}

		if err := q.Ack(tx, id1, id2, id3, id4); s.NoError(err) {
			if task, err := q.Task(tx, id3); s.NoError(err) {
				s.Equal(orm.StatusConfirmed, task.Status())
			}

			if lost, err := q.Lost(tx, 100); s.NoError(err) {
				s.Len(lost, 0)
			}
		}
	}

	if wait, work, err := q.Stat(tx); s.NoError(err) {
		s.Equal(int64(0), wait)
		s.Equal(int64(0), work)
	}

	s.Require().NoError(tx.Commit())

	// Удаляем строки, чтобы автовакуум их собрал
	tx = mvcc.Begin(s.cn)
	s.Require().NoError(s.tbl.Select(tx).Delete())
	s.Require().NoError(tx.Commit())

	s.checkVacuum(map[string]bool{
		"\\x00\\xaa\\xaa\\x03\\xcc\\xcclox\\x00trigger": true,
		"\\x00\\xaa\\xaa\\x03\\xcc\\xcclox\\x00wait":    true,
		"\\x00\\xaa\\xaa\\x03\\xcc\\xcclox\\x00work":    true,
	})
}

func (s *ORMSuite) TestWhereLimit() {
	const recCount = 3

	id1 := fdb.Key("id1")
	id2 := fdb.Key("id2")
	id3 := fdb.Key("id3")
	id4 := fdb.Key("id4")
	id5 := fdb.Key("id5")
	id6 := fdb.Key("id6")

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdb.KeyValue{id1, []byte("msg1 true")},
		fdb.KeyValue{id2, []byte("txt2 false")},
		fdb.KeyValue{id3, []byte("msg3 false")},
		fdb.KeyValue{id4, []byte("msg4 true")},
		fdb.KeyValue{id5, []byte("txt5 false")},
		fdb.KeyValue{id6, []byte("txt6 true")},
	))

	f1 := func(p fdb.KeyValue) (need bool, err error) {
		return strings.HasSuffix(string(p.Value), "true"), nil
	}

	f2 := func(p fdb.KeyValue) (need bool, err error) {
		return strings.HasPrefix(string(p.Value), "msg"), nil
	}

	if list, err := s.tbl.Select(s.tx).Where(f1).Where(f2).All(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id1", list[0].Key.String())
		s.Equal("msg1 true", string(list[0].Value))

		s.Equal("id4", list[1].Key.String())
		s.Equal("msg4 true", string(list[1].Value))
	}

	if list, err := s.tbl.Select(s.tx).Where(f2).Limit(2).Reverse().All(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id4", list[0].Key.String())
		s.Equal("msg4 true", string(list[0].Value))

		s.Equal("id3", list[1].Key.String())
		s.Equal("msg3 false", string(list[1].Value))
	}
}

func (s *ORMSuite) TestMultiIndex() {
	id1 := fdb.Key("id1")
	id2 := fdb.Key("id2")
	id3 := fdb.Key("id3")

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdb.KeyValue{id1, []byte("message")},
		fdb.KeyValue{id2, []byte("text")},
		fdb.KeyValue{id3, []byte("mussage")},
	))
	s.Require().NoError(s.tx.Commit())

	tx := mvcc.Begin(s.cn)
	defer tx.Cancel()

	query := s.tbl.Select(tx).ByIndex(TestIndex2, fdb.Key("ss"))

	if list, err := query.All(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id1", list[0].Key.String())
		s.Equal("message", string(list[0].Value))

		s.Equal("id3", list[1].Key.String())
		s.Equal("mussage", string(list[1].Value))
	}
}

func (s *ORMSuite) TestSubList() {
	id1 := fdb.Key("id1")
	id2 := fdb.Key("id2")
	id3 := fdb.Key("id3")

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdb.KeyValue{id1, []byte("message")},
		fdb.KeyValue{id2, []byte("text")},
		fdb.KeyValue{id3, []byte("mussage")},
	))
	s.Require().NoError(s.tx.Commit())

	q := orm.NewQueue(TestQueue, s.tbl, orm.Refresh(10*time.Millisecond), orm.Prefix([]byte("lox")))

	// Публикация задач по очереди (и нарочно одну и ту же несколько раз)
	tx := mvcc.Begin(s.cn)
	s.Require().NoError(q.PubList(tx, []fdb.Key{id1, id1, id2, id2, id3}, orm.Delay(time.Millisecond)))
	s.Require().NoError(tx.Commit())

	// Запрашиваем несколько задач, с ограничением
	tx = mvcc.Begin(s.cn)

	// На все про все даем пару секунд, этого более, чем достаточно
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// В первой пачке должно быть 2 задачи, потому их там больше, чем мы просим
	if list, err := q.SubList(ctx, s.cn, 2); s.NoError(err) && s.Len(list, 2) {
		s.Equal(id1, list[0].Key())
		s.Equal(id2, list[1].Key())
	}

	// Во второй пачке должна быть 1 задача, потому их мы не ждем наполнения, а выходим сразу, как что-то есть
	if list, err := q.SubList(ctx, s.cn, 2); s.NoError(err) && s.Len(list, 1) {
		s.Equal(id3, list[0].Key())
	}

	start := time.Now()
	wctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// Наконец, в 3 пачке не должно быть ничего
	// Кроме того, мы ставим маленький контекст
	if _, err := q.SubList(wctx, s.cn, 2000); s.Error(err) {
		s.True(errx.Is(err, context.DeadlineExceeded))
	}

	// Важно убедиться, что мы не ждем у моря погоды, а выходим сразу, как просрачился контекст
	s.True(time.Since(start) < time.Second)
}

func (s *ORMSuite) TestCursor() {
	const recCount = 3

	id1 := fdb.Key("id1")
	id2 := fdb.Key("id2")
	id3 := fdb.Key("id3")
	id4 := fdb.Key("id4")
	id5 := fdb.Key("id5")
	id6 := fdb.Key("id6")

	s.Require().NoError(s.tbl.Upsert(s.tx,
		fdb.KeyValue{id1, []byte("msg1")},
		fdb.KeyValue{id2, []byte("txt2")},
		fdb.KeyValue{id3, []byte("msg3")},
		fdb.KeyValue{id4, []byte("txt4")},
		fdb.KeyValue{id5, []byte("msg5")},
		fdb.KeyValue{id6, []byte("msg6")},
	))
	s.Require().NoError(s.tx.Commit())

	// Проверка курсора по индексу прямо

	tx := mvcc.Begin(s.cn)

	query := s.tbl.Select(tx).ByIndex(TestIndex, fdb.Key("msg"))

	if list, err := query.Page(2).Next(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id1", list[0].Key.String())
		s.Equal("msg1", string(list[0].Value))

		s.Equal("id3", list[1].Key.String())
		s.Equal("msg3", string(list[1].Value))
	}

	qid, err := query.Save()
	s.Require().NoError(err)
	s.Require().NoError(tx.Commit())

	tx = mvcc.Begin(s.cn)

	query, err = s.tbl.Cursor(tx, qid)
	s.Require().NoError(err)

	if list, err := query.Next(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id5", list[0].Key.String())
		s.Equal("msg5", string(list[0].Value))

		s.Equal("id6", list[1].Key.String())
		s.Equal("msg6", string(list[1].Value))
	}

	// Проверка курсора по индексу с реверсом

	query = s.tbl.Select(tx).ByIndex(TestIndex, fdb.Key("msg")).Reverse()

	if list, err := query.Page(2).Next(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id6", list[0].Key.String())
		s.Equal("msg6", string(list[0].Value))

		s.Equal("id5", list[1].Key.String())
		s.Equal("msg5", string(list[1].Value))
	}

	qid, err = query.Save()
	s.Require().NoError(err)
	s.Require().NoError(tx.Commit())

	tx = mvcc.Begin(s.cn)

	query, err = s.tbl.Cursor(tx, qid)
	s.Require().NoError(err)

	if list, err := query.Next(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id3", list[0].Key.String())
		s.Equal("msg3", string(list[0].Value))

		s.Equal("id1", list[1].Key.String())
		s.Equal("msg1", string(list[1].Value))
	}

	// Проверка курсора по коллекции прямо

	tx = mvcc.Begin(s.cn)

	query = s.tbl.Select(tx)

	if list, err := query.Page(2).Next(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id1", list[0].Key.String())
		s.Equal("msg1", string(list[0].Value))

		s.Equal("id2", list[1].Key.String())
		s.Equal("txt2", string(list[1].Value))
	}

	qid, err = query.Save()
	s.Require().NoError(err)
	s.Require().NoError(tx.Commit())

	tx = mvcc.Begin(s.cn)

	query, err = s.tbl.Cursor(tx, qid)
	s.Require().NoError(err)

	if list, err := query.Next(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id3", list[0].Key.String())
		s.Equal("msg3", string(list[0].Value))

		s.Equal("id4", list[1].Key.String())
		s.Equal("txt4", string(list[1].Value))
	}

	// Проверка курсора по коллекции с реверсом
	query = s.tbl.Select(tx).Reverse()

	if list, err := query.Page(2).Next(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id6", list[0].Key.String())
		s.Equal("msg6", string(list[0].Value))

		s.Equal("id5", list[1].Key.String())
		s.Equal("msg5", string(list[1].Value))
	}

	qid, err = query.Save()
	s.Require().NoError(err)
	s.Require().NoError(tx.Commit())

	tx = mvcc.Begin(s.cn)

	query, err = s.tbl.Cursor(tx, qid)
	s.Require().NoError(err)

	if list, err := query.Next(); s.NoError(err) && s.Len(list, 2) {
		s.Equal("id4", list[0].Key.String())
		s.Equal("txt4", string(list[0].Value))

		s.Equal("id3", list[1].Key.String())
		s.Equal("msg3", string(list[1].Value))
	}

	s.Require().NoError(tx.Commit())
}

func BenchmarkUpsert(b *testing.B) {
	cn, err := db.Connect(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx := mvcc.Begin(cn)
	defer tx.Cancel()

	tbl := orm.NewTable(TestTable)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		uid := []byte(typex.NewUUID())
		require.NoError(b, tbl.Upsert(tx, fdb.KeyValue{fdb.Key(uid), uid}))
	}
}

func BenchmarkUpsertBatch(b *testing.B) {
	cn, err := db.Connect(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx := mvcc.Begin(cn)
	defer tx.Cancel()

	tbl := orm.NewTable(TestTable)

	count := 1000
	batch := make([]fdb.KeyValue, count)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for i := range batch {
			uid := []byte(typex.NewUUID())
			batch[i] = fdb.KeyValue{fdb.Key(uid), uid}
		}
		require.NoError(b, tbl.Upsert(tx, batch...))
	}
}

func BenchmarkCount(b *testing.B) {
	// const count = 1000000
	// batchSize := 10000
	// mvcc.ScanRangeSize = 100000

	const count = 400000
	batchSize := 10000

	cn, err := db.Connect(TestDB)

	require.NoError(b, err)
	require.NoError(b, cn.Clear())

	tx := mvcc.Begin(cn)

	tbl := orm.NewTable(TestTable)

	for k := 0; k < count/batchSize; k++ {
		batch := make([]fdb.KeyValue, batchSize)
		for i := 0; i < batchSize; i++ {
			uid := []byte(typex.NewUUID())
			batch[i] = fdb.KeyValue{uid, uid}
		}
		require.NoError(b, tbl.Upsert(tx, batch...))
	}

	require.NoError(b, tx.Commit())

	b.Run("Count", func(br *testing.B) {
		tx := mvcc.Begin(cn)
		defer tx.Cancel()

		for i := 0; i < br.N; i++ {
			cnt := uint64(0)
			if tbl.Select(tx).Agg(orm.Count(&cnt)) != nil {
				b.Fatalf("err = %+v", err)
			}
			if int(cnt) != count {
				b.Fatalf("act %d != exp %d", cnt, count)
			}
		}
	})
}
