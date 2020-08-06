package db

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

/*
	ConnectV610 - создание нового подключения к серверу FDB и базе данных.

	Идентификатор базы всего 1 байт, потому что пока не рассчитываем на то, что разных БД будет так много.
	Особое значение 0xFF (255) запрещено, т.к. с этого байта начинается служебная область видимости FDB.

	Если указан путь к файлу, то подключается к нему. Иначе идет по стандартному (зависит от ОС).

	Этот драйвер настроен на совместимость с конкретной версией клиента, с другими может не заработать.
*/
func ConnectV610(databaseID byte, clusterFile ...string) (_ Connection, err error) {
	const verID = 610
	const badID = "Invalid database ID: %X"

	if databaseID == 0xFF {
		return nil, ErrBadDB.WithDetail(badID, databaseID)
	}

	if err = fdb.APIVersion(verID); err != nil {
		return nil, ErrConnect.WithReason(err)
	}

	conn := &v610Conn{
		databaseID: databaseID,
	}

	if len(clusterFile) > 0 {
		if conn.Database, err = fdb.OpenDatabase(clusterFile[0]); err != nil {
			return nil, ErrConnect.WithReason(err)
		}
	} else {
		if conn.Database, err = fdb.OpenDefault(); err != nil {
			return nil, ErrConnect.WithReason(err)
		}
	}

	return conn, nil
}

type v610Conn struct {
	fdb.Database

	databaseID byte
}

func (cn *v610Conn) Read(hdl func(Reader) error) error {
	if _, err := cn.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		return nil, hdl(&v610Reader{cn: cn, tx: tx})
	}); err != nil {
		return ErrRead.WithReason(err)
	}
	return nil
}

func (cn *v610Conn) Write(hdl func(Writer) error) error {
	if _, err := cn.Transact(func(tx fdb.Transaction) (interface{}, error) {
		return nil, hdl(&v610Writer{v610Reader: &v610Reader{cn: cn, tx: tx}, tx: tx})
	}); err != nil {
		return ErrWrite.WithReason(err)
	}
	return nil
}

func (cn *v610Conn) Clear() error {
	if _, err := cn.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.ClearRange(fdb.KeyRange{
			Begin: fdb.Key{cn.databaseID},
			End:   fdb.Key{cn.databaseID, 0xFF},
		})
		return nil, nil
	}); err != nil {
		return ErrClear.WithReason(err)
	}
	return nil
}

func (cn *v610Conn) Serial(ctx context.Context, ns byte, from, to []byte, limit int, reverse bool) (<-chan *Pair, <-chan error) {
	list := make(chan *Pair)
	errs := make(chan error, 1)

	go func() {
		var err error
		var fFrom, fTo fdb.Key

		defer close(list)
		defer close(errs)

		if fFrom, err = cn.fdbKey(ns, from); err != nil {
			errs <- ErrSerial.WithReason(err)
			return
		}

		if fTo, err = cn.fdbKey(ns, to); err != nil {
			errs <- ErrSerial.WithReason(err)
			return
		}

		if _, err = cn.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			opts := fdb.RangeOptions{Mode: fdb.StreamingModeIterator, Reverse: reverse, Limit: limit}
			rng := tx.GetRange(fdb.KeyRange{Begin: fFrom, End: fTo}, opts).Iterator()

			for rng.Advance() {
				pair := rng.MustGet()

				select {
				case list <- &Pair{
					Key:   cn.usrKey(pair.Key),
					Value: pair.Value,
				}:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		}); err != nil {
			errs <- ErrSerial.WithReason(err)
			return
		}
	}()

	return list, errs
}

func (cn *v610Conn) fdbKey(ns byte, key []byte) (res fdb.Key, err error) {
	const badNS = "Invalid namespace ID: %X"

	if ns == 0xFF {
		return nil, ErrBadNS.WithDetail(badNS, ns)
	}

	// TODO: исследовать возможность использования sync.Pool
	res = make(fdb.Key, 2+len(key))
	res[0] = cn.databaseID
	res[1] = byte(ns)
	copy(res[2:], key)
	return res, nil
}

func (cn *v610Conn) usrKey(key fdb.Key) []byte { return key[2:] }

type v610Reader struct {
	cn *v610Conn
	tx fdb.ReadTransaction
}

func (r *v610Reader) Pair(ns byte, key []byte) (res *Pair, err error) {
	var fk fdb.Key

	if fk, err = r.cn.fdbKey(ns, key); err != nil {
		return nil, ErrGetPair.WithReason(err)
	}

	res = &Pair{Key: key}

	if res.Value, err = r.tx.Get(fk).Get(); err != nil {
		return nil, ErrGetPair.WithReason(err)
	}

	return res, nil
}

func (r *v610Reader) List(ns byte, from, to []byte, limit int, reverse bool) (res []*Pair, err error) {
	var fFrom, fTo fdb.Key

	if fFrom, err = r.cn.fdbKey(ns, from); err != nil {
		return nil, ErrGetList.WithReason(err)
	}

	if fTo, err = r.cn.fdbKey(ns, to); err != nil {
		return nil, ErrGetList.WithReason(err)
	}

	opts := fdb.RangeOptions{Mode: fdb.StreamingModeWantAll, Reverse: reverse, Limit: limit}
	rng := r.tx.GetRange(fdb.KeyRange{Begin: fFrom, End: fTo}, opts).GetSliceOrPanic()
	res = make([]*Pair, len(rng))

	for i := range rng {
		res[i] = &Pair{
			Key:   r.cn.usrKey(rng[i].Key),
			Value: rng[i].Value,
		}
	}

	return res, nil
}

type v610Writer struct {
	*v610Reader
	tx fdb.Transaction
}

func (w *v610Writer) SetPair(ns byte, key, value []byte) (err error) {
	var fk fdb.Key

	if fk, err = w.cn.fdbKey(ns, key); err != nil {
		return ErrSetPair.WithReason(err)
	}

	w.tx.Set(fk, value)
	return nil
}

func (w *v610Writer) SetVersion(ns byte, key []byte) (err error) {
	var fk fdb.Key
	var value [14]byte

	if fk, err = w.cn.fdbKey(ns, key); err != nil {
		return ErrSetVersion.WithReason(err)
	}

	w.tx.SetVersionstampedValue(fk, value[:])
	return nil
}

func (w *v610Writer) DropPair(ns byte, key []byte) (err error) {
	var fk fdb.Key

	if fk, err = w.cn.fdbKey(ns, key); err != nil {
		return ErrDropPair.WithReason(err)
	}

	w.tx.Clear(fk)
	return nil
}
