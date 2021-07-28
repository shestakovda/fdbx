package db

import (
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/shestakovda/fdbx/v2"
)

// Connect - создание нового подключения к серверу FDB и базе данных.
//
// Идентификатор базы всего 1 байт, потому что пока не рассчитываем на то, что разных БД будет так много.
// Особое значение 0xFF (255) запрещено, т.к. с этого байта начинается служебная область видимости FDB.
//
// Если указан путь к файлу, то подключается к нему. Иначе идет по стандартному (зависит от ОС).
//
// Этот драйвер настроен на совместимость с конкретной версией клиента, с другими может не заработать.
func Connect(id byte, opts ...Option) (cn Connection, err error) {
	const verID = 620
	const badID = "Invalid database ID: %X"

	if id == 0xFF {
		return cn, ErrConnect.WithDetail(badID, id)
	}

	cn = Connection{
		ID: id,
	}

	for i := range opts {
		if err = opts[i](&cn.options); err != nil {
			return cn, ErrConnect.WithReason(err)
		}
	}

	if cn.WithMetrics {
		start := time.Now()
		OpsCounter.WithLabelValues(opConnect).Inc()
		CallsCounter.WithLabelValues(opConnect).Inc()
		defer func() {
			CallsHistogram.WithLabelValues(opConnect).Observe(time.Since(start).Seconds())
			if err != nil {
				ErrorsCounter.WithLabelValues(opConnect).Inc()
			}
		}()
	}

	if err = fdb.APIVersion(verID); err != nil {
		return cn, ErrConnect.WithReason(err)
	}

	if len(cn.ClusterFile) > 0 {
		if cn.db, err = fdb.OpenDatabase(cn.ClusterFile); err != nil {
			return cn, ErrConnect.WithReason(err)
		}
	} else {
		if cn.db, err = fdb.OpenDefault(); err != nil {
			return cn, ErrConnect.WithReason(err)
		}
	}

	cn.ok = true
	return cn, nil
}

type Connection struct {
	ID byte

	options
	ok bool
	db fdb.Database
}

func (cn Connection) Empty() bool { return !cn.ok }

func (cn Connection) Read(hdl ReadHandler) error {
	if cn.WithMetrics {
		start := time.Now()
		CallsCounter.WithLabelValues(opRead).Inc()
		defer func() { CallsHistogram.WithLabelValues(opRead).Observe(time.Since(start).Seconds()) }()
	}
	if _, err := cn.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		if cn.WithMetrics {
			OpsCounter.WithLabelValues(opRead).Inc()
		}
		return nil, hdl(Reader{Connection: cn, tx: tx})
	}); err != nil {
		if cn.WithMetrics {
			ErrorsCounter.WithLabelValues(opRead).Inc()
		}
		return ErrRead.WithReason(err)
	}
	return nil
}

func (cn Connection) Write(hdl WriteHandler) error {
	if cn.WithMetrics {
		start := time.Now()
		CallsCounter.WithLabelValues(opWrite).Inc()
		defer func() { CallsHistogram.WithLabelValues(opWrite).Observe(time.Since(start).Seconds()) }()
	}
	if _, err := cn.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		if cn.WithMetrics {
			OpsCounter.WithLabelValues(opWrite).Inc()
		}
		return nil, hdl(Writer{Reader: Reader{Connection: cn, tx: tx}, tx: tx})
	}); err != nil {
		if cn.WithMetrics {
			ErrorsCounter.WithLabelValues(opWrite).Inc()
		}
		return ErrWrite.WithReason(err)
	}
	return nil
}

func (cn Connection) Clear() error {
	if cn.WithMetrics {
		start := time.Now()
		CallsCounter.WithLabelValues(opWrite).Inc()
		defer func() { CallsHistogram.WithLabelValues(opWrite).Observe(time.Since(start).Seconds()) }()
	}
	if _, err := cn.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		if cn.WithMetrics {
			OpsCounter.WithLabelValues(opWrite).Inc()
		}
		tx.ClearRange(fdb.KeyRange{Begin: cn.usrWrap(nil), End: cn.endWrap(nil)})
		return nil, nil
	}); err != nil {
		if cn.WithMetrics {
			ErrorsCounter.WithLabelValues(opWrite).Inc()
		}
		return ErrClear.WithReason(err)
	}
	return nil
}

func (cn Connection) usrWrap(key fdb.Key) fdb.KeyConvertible {
	if key == nil {
		return fdb.Key{cn.ID}
	}

	return fdbx.AppendLeft(key, cn.ID)
}

func (cn Connection) endWrap(key fdb.Key) fdb.KeyConvertible {
	if key == nil {
		return fdbx.AppendLeft(tail, cn.ID)
	}

	return fdbx.AppendLeft(fdbx.AppendRight(key, tail...), cn.ID)
}
