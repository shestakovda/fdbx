package db

import "github.com/apple/foundationdb/bindings/go/src/fdb"

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

func (v *v610Conn) Read(hdl func(Reader) error) error {
	if _, err := v.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		return nil, hdl(&v610Reader{cn: v, tx: tx})
	}); err != nil {
		return ErrRead.WithReason(err)
	}
	return nil
}

func (v *v610Conn) Write(hdl func(Writer) error) error {
	if _, err := v.Transact(func(tx fdb.Transaction) (interface{}, error) {
		return nil, hdl(&v610Writer{v610Reader: &v610Reader{cn: v, tx: tx}, tx: tx})
	}); err != nil {
		return ErrWrite.WithReason(err)
	}
	return nil
}

func (v *v610Conn) Clear() error {
	if _, err := v.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.ClearRange(fdb.KeyRange{
			Begin: fdb.Key{v.databaseID},
			End:   fdb.Key{v.databaseID, 0xFF},
		})
		return nil, nil
	}); err != nil {
		return ErrClear.WithReason(err)
	}
	return nil
}

type v610Reader struct {
	cn *v610Conn
	tx fdb.ReadTransaction
}

func (r *v610Reader) fdbKey(ns byte, key []byte) (res fdb.Key, err error) {
	const badNS = "Invalid namespace ID: %X"

	if ns == 0xFF {
		return nil, ErrBadNS.WithDetail(badNS, ns)
	}

	// TODO: исследовать возможность использования sync.Pool
	res = make(fdb.Key, 2+len(key))
	res[0] = r.cn.databaseID
	res[1] = byte(ns)
	copy(res[2:], key)
	return res, nil
}

func (r *v610Reader) usrKey(key fdb.Key) []byte {
	return key[2:]
}

func (r *v610Reader) Pair(ns byte, key []byte) (res *Pair, err error) {
	var fk fdb.Key

	if fk, err = r.fdbKey(ns, key); err != nil {
		return nil, ErrGetPair.WithReason(err)
	}

	res = &Pair{Key: key}

	if res.Value, err = r.tx.Get(fk).Get(); err != nil {
		return nil, ErrGetPair.WithReason(err)
	}

	return res, nil
}

func (r *v610Reader) List(ns byte, prefix []byte, limit int, reverse bool) (res []*Pair, err error) {
	var fk fdb.Key

	if fk, err = r.fdbKey(ns, prefix); err != nil {
		return nil, ErrGetList.WithReason(err)
	}

	rng := r.tx.GetRange(fdb.KeyRange{
		Begin: fk,
		End:   fdb.Key(append(fk, 0xFF)),
	}, fdb.RangeOptions{
		Mode:    fdb.StreamingModeWantAll,
		Reverse: reverse,
		Limit:   limit,
	}).GetSliceOrPanic()

	res = make([]*Pair, len(rng))

	for i := range rng {
		res[i] = &Pair{
			Key:   r.usrKey(rng[i].Key),
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

	if fk, err = w.fdbKey(ns, key); err != nil {
		return ErrSetPair.WithReason(err)
	}

	w.tx.Set(fk, value)
	return nil
}

func (w *v610Writer) SetVersion(ns byte, key []byte) (err error) {
	var fk fdb.Key
	var value [14]byte

	if fk, err = w.fdbKey(ns, key); err != nil {
		return ErrSetVersion.WithReason(err)
	}

	w.tx.SetVersionstampedValue(fk, value[:])
	return nil
}

func (w *v610Writer) DropPair(ns byte, key []byte) (err error) {
	var fk fdb.Key

	if fk, err = w.fdbKey(ns, key); err != nil {
		return ErrDropPair.WithReason(err)
	}

	w.tx.Clear(fk)
	return nil
}
