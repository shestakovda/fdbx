package db

import "github.com/apple/foundationdb/bindings/go/src/fdb"

/*
	NewConnV610 - создание нового подключения к серверу FDB и базе данных.

	Идентификатор базы всего 1 байт, потому что пока не рассчитываем на то, что разных БД будет так много.
	Особое значение 0xFF (255) запрещено, т.к. с этого байта начинается служебная область видимости FDB.

	Если указан путь к файлу, то подключается к нему. Иначе идет по стандартному (зависит от ОС).

	Этот драйвер настроен на совместимость с конкретной версией клиента, с другими может не заработать.
*/
func NewConnV610(databaseID byte, clusterFile ...string) (Connection, error) {
	const verID = 610
	const badID = "Invalid database ID: %X"

	if databaseID == 0xFF {
		return nil, ErrSchema.WithDetail(badID, databaseID)
	}

	if err = fdb.APIVersion(verID); err != nil {
		return ErrConnect.WithReason(err)
	}

	conn := &v610Conn{
		databaseID: databaseID,
	}

	if len(clusterFile) > 0 {
		if conn.Database, err = fdb.OpenDatabase(clusterFile[0]); err != nil {
			return ErrConnect.WithReason(err)
		}
	} else {
		if conn.Database, err = fdb.OpenDefault(); err != nil {
			return ErrConnect.WithReason(err)
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

func (v *v610Conn) Write(hdl func(Reader) error) error {
	if _, err := v.Transact(func(tx fdb.Transaction) (interface{}, error) {
		return nil, hdl(&v610Writer{rd: &v610Reader{cn: v, tx: tx}, tx: tx})
	}); err != nil {
		return ErrWrite.WithReason(err)
	}
	return err
}

type v610Reader struct {
	cn *v610Conn
	tx fdb.ReadTransaction
}

func (r *v610Reader) fdbKey(ns byte, key []byte) (res fdb.Key, err error) {
	const badNS = "Invalid namespace ID: %X"

	if ns == 0xFF {
		return nil, ErrSchema.WithDetail(badNS, ns)
	}

	// TODO: исследовать возможность использования sync.Pool
	res = make(fdb.Key, 2+len(key))
	res[0] = r.cn.databaseID
	res[1] = ns
	copy(res[2:], key)
	return res, nil
}

func (r *v610Reader) Load(ns byte, key []byte) (_ Pair, err error) {
	var fk fdb.Key
	var val []byte

	if fk, err = r.fdbKey(ns, key); err != nil {
		return
	}

	if val, err = r.tx.Get(fk).Get(); err != nil {
		return ErrRead.WithReason(err)
	}

	return NewBytePair(ns, key, val), nil
}

func (r *v610Reader) List(ns byte, prefix []byte, reverse bool) ([]Pair, error) {
	return nil, nil
}

type v610Writer struct {
	rd *v610Reader
	tx fdb.Transaction
}

func (w *v610Writer) SetPair(p Pair) error {
	return nil
}

func (w *v610Writer) SetVersion(p Pair) (err error) {
	var fk fdb.Key
	var value [14]byte

	if fk, err = r.fdbKey(p.NS(), p.Key()); err != nil {
		return
	}

	w.tx.SetVersionstampedValue(fk, value[:])
	return nil
}
