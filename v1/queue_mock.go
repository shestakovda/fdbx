package fdbx

import (
	"context"
	"time"
)

type mockQueue struct {
	*MockConn
}

func (q *mockQueue) Ack(db DB, id ...string) error                         { return q.FAck(db, id...) }
func (q *mockQueue) Pub(db DB, t time.Time, id ...string) error            { return q.FPub(db, t, id...) }
func (q *mockQueue) Sub(ctx context.Context) (<-chan Record, <-chan error) { return q.FSub(ctx) }
func (q *mockQueue) SubOne(ctx context.Context) (Record, error)            { return q.FSubOne(ctx) }
func (q *mockQueue) GetLost(limit uint, cond Condition) ([]Record, error) {
	return q.FGetLost(limit, cond)
}
func (q *mockQueue) Status(db DB, ids ...string) (map[string]TaskStatus, error) {
	return q.FStatus(db, ids...)
}
func (q *mockQueue) SubList(ctx context.Context, limit uint) ([]Record, error) {
	return q.FSubList(ctx, limit)
}
func (q *mockQueue) Stat() (wait, lost int, err error) {
	return q.FStat()
}
