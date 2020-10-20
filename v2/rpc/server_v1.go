package rpc

import (
	"context"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2/db"
)

func newServerV1(list []*Listener) Server {
	s := v1Server{
		list: list,
	}

	return &s
}

type v1Server struct {
	list []*Listener
	wait *sync.WaitGroup
	exit context.CancelFunc
}

func (s *v1Server) Run(ctx context.Context, cn db.Connection) (err error) {
	var wctx context.Context

	wctx, s.exit = context.WithCancel(ctx)
	s.wait = new(sync.WaitGroup)

	// Сначала все проверяем
	for i := range s.list {
		if err = s.list[i].check(); err != nil {
			return
		}
	}

	// Затем все запускаем
	s.wait.Add(len(s.list))
	for i := range s.list {
		go s.listen(wctx, cn, s.list[i])
	}

	return nil
}

func (s *v1Server) Stop() {
	if s.exit != nil {
		s.exit()
	}

	s.wait.Wait()
}

func (s *v1Server) listen(ctx context.Context, cn db.Connection, l *Listener) {
	var err error

	defer func() {
		// Перезапуск только в случае ошибки
		if err != nil {
			// Которая обработана и требует перезапуска
			if repeat, wait := l.OnListen(err); repeat {
				// Возможно, не сразу готовы обрабатывать снова
				if wait > 0 {
					time.Sleep(wait)
				}

				// И только если мы вообще можем еще запускать
				if ctx.Err() == nil {
					glog.Errorf("=-=-=222 %p", l)
					// Тогда стартуем заново и в s.wait ничего не ставим
					go s.listen(ctx, cn, l)
					return
				}
			}
		}

		// В остальных случаях, нечего ловить, закрываем ожидание
		s.wait.Done()
	}()

	// Отлавливаем панику и превращаем в ошибку
	defer func() {
		if rec := recover(); rec != nil {
			if e, ok := rec.(error); ok {
				err = ErrListen.WithReason(e)
			} else {
				err = ErrListen.WithDebug(errx.Debug{"panic": rec})
			}
		}
	}()

	// Собственный контекст для гарантированного завершения подписки в случае провала
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pairs, errs := l.Queue.Sub(wctx, cn, 1)

	for pair := range pairs {
		glog.Errorf("=-=-= %p", l)
		glog.Errorf("=-=-= %p", pair)
		// В случае ошибки при обработке задачи
		if exp := l.OnTask(pair); exp != nil {
			// Обрабатываем ошибку и если нужно, повторяем задачу
			if repeat, wait := l.OnError(exp); repeat {
				// Если не смогли повторить - это фиаско
				if err = l.repeat(cn, pair, wait); err != nil {
					err = ErrListen.WithReason(err)
					return
				}
			}
		} else {
			if err = l.ack(cn, pair); err != nil {
				err = ErrListen.WithReason(err)
				return
			}
		}
	}

	for exp := range errs {
		if exp != nil && !errx.Is(exp, context.Canceled, context.DeadlineExceeded) {
			err = ErrListen.WithReason(exp)
			return
		}
	}

}
