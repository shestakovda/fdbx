package fdbx

import (
	"context"
	"time"
)

func newMockQueue(conn *MockConn, rtp RecordType, prefix string) (*mockQueue, error) {
	return &mockQueue{MockConn: conn, rtp: rtp, pf: prefix}, nil
}

type mockQueue struct {
	*MockConn
	pf  string
	rtp RecordType
}

func (q *mockQueue) Ack(db DB, id ...string) error                         { return q.FAck(db, id...) }
func (q *mockQueue) Pub(db DB, t time.Time, id ...string) error            { return q.FPub(db, t, id...) }
func (q *mockQueue) Sub(ctx context.Context) (<-chan Record, <-chan error) { return q.FSub(ctx) }
func (q *mockQueue) SubOne(ctx context.Context) (Record, error)            { return q.FSubOne(ctx) }
func (q *mockQueue) GetLost(limit uint, filter Predicat) ([]Record, error) {
	return q.FGetLost(limit, filter)
}
func (q *mockQueue) CheckLost(db DB, ids ...string) (map[string]bool, error) {
	return q.FCheckLost(db, ids...)
}
func (q *mockQueue) SubList(ctx context.Context, limit uint) ([]Record, error) {
	return q.FSubList(ctx, limit)
}
func (q *mockQueue) Stat() (wait, lost int, err error) {
	return q.FStat()
}
