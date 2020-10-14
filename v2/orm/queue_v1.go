package orm

import (
	"context"
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/typex"
)

const (
	qFlag byte = 0
	qList byte = 1
	qWork byte = 2
)

var qTriggerKey = fdbx.Key("trigger").LPart(qFlag)

func Queue(id byte, cl Collection, opts ...Option) TaskCollection {
	q := v1Queue{
		id:      id,
		cl:      cl,
		options: newOptions(),
	}

	for i := range opts {
		opts[i](&q.options)
	}

	return &q
}

type v1Queue struct {
	options
	id byte
	cl Collection
}

func (q v1Queue) ID() byte { return q.id }

func (q v1Queue) Pub(tx mvcc.Tx, when time.Time, ids ...fdbx.Key) (err error) {

	if when.IsZero() {
		when = time.Now()
	}

	delay := make([]byte, 8)
	binary.BigEndian.PutUint64(delay, uint64(when.UTC().UnixNano()))

	// Структура ключа:
	// db nsUser cl.id q.id qList delay uid = taskID
	wrp := func(key fdbx.Key) (fdbx.Key, error) { return key.LPart(delay...).LPart(qList), nil }

	pairs := make([]fdbx.Pair, len(ids))
	for i := range ids {
		pairs[i] = fdbx.NewPair(
			fdbx.Key(typex.NewUUID()), // Случайная айдишка таски, чтобы не было конфликтов при одинаковом времени
			fdbx.Value(ids[i]),        // Айдишку элемента очереди записываем в значение, именно она и является таской
		).WrapKey(wrp).WrapKey(q.usrKeyWrapper)
	}

	if err = tx.Upsert(pairs); err != nil {
		return ErrPub.WithReason(err)
	}

	// Особая магия - инкремент счетчика очереди, чтобы затриггерить подписчиков
	if err = tx.Conn().Write(func(w db.Writer) error {
		w.Increment(q.usrKey(qTriggerKey), 1)
		return nil
	}); err != nil {
		return ErrPub.WithReason(err)
	}

	return nil
}

func (q v1Queue) SubList(ctx context.Context, tx mvcc.Tx, limit int) (list []fdbx.Pair, err error) {
	if limit == 0 {
		return nil, nil
	}

	var id fdbx.Value
	var waiter db.Waiter
	var pairs []fdbx.Pair

	from := q.usrKey(fdbx.Key{qList})

	for {

		if err = q.waitTask(ctx, waiter); err != nil {
			return
		}

		now := make([]byte, 8)
		binary.BigEndian.PutUint64(now, uint64(time.Now().UTC().UnixNano()))
		to := q.usrKey(fdbx.Key(now).LPart(qList))

		// Значения в этих парах - айдишки элементов коллекции
		if pairs, err = tx.SeqScan(from, to, mvcc.Limit(limit), mvcc.Exclusive()); err != nil {
			return nil, ErrSub.WithReason(err)
		}

		if len(pairs) == 0 {
			if err = tx.Conn().Write(func(w db.Writer) error {
				waiter = w.Watch(q.usrKey(qTriggerKey))
				return nil
			}); err != nil {
				return nil, ErrSub.WithReason(err)
			}
			continue
		}

		ids := make([]fdbx.Key, len(pairs))

		for i := range pairs {
			if id, err = pairs[i].Value(); err != nil {
				return nil, ErrSub.WithReason(err)
			}
			ids[i] = fdbx.Key(id)
		}

		if list, err = q.cl.Select(tx).PossibleByID(ids...).All(); err != nil {
			return nil, ErrSub.WithReason(err)
		}

		return list, nil
	}
}

func (q v1Queue) waitTask(ctx context.Context, waiter db.Waiter) (err error) {
	if waiter == nil {
		return nil
	}

	if ctx.Err() != nil {
		return ErrSub.WithReason(ctx.Err())
	}

	// Даже если waiter установлен, то при отсутствии других публикаций мы тут зависнем навечно.
	// А задачи, время которых настало, будут просрочены. Для этого нужен особый механизм обработки по таймауту.
	wctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if err = waiter.Resolve(wctx); err != nil {
		return ErrSub.WithReason(err)
	}

	// Если запущено много обработчиков, все они рванут забирать события одновременно.
	// Чтобы избежать массовых конфликтов транзакций и улучшить распределение задач делаем небольшую
	// случайную задержку, в пределах 20 мс. Немного для человека, значительно для уменьшения конфликтов
	time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
	return nil
}

func (q v1Queue) usrKeyWrapper(key fdbx.Key) (fdbx.Key, error) { return q.usrKey(key), nil }

func (q v1Queue) usrKey(key fdbx.Key) fdbx.Key {
	clid := q.cl.ID()
	return key.LPart(byte(clid>>8), byte(clid), q.id)
}
