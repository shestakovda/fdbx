## FoundationDB eXtended v1

**ALARM: Do not use in production! This experimental version is obsoleted by v2!**

### About

With `Record` interface you can store any data in collections. 

With `Cursor` interface you can select it back. 

With `Queue` interface you can organize async processes between different workers.

With `DB` interface you can rely on transactions.

With `Conn` interface you can do all of this magic together.

## Usage

### Minimal requirements

We should implement Record interface:

```golang

// we use collection number instead string names
const MyFirstCollection = uint16(1)

type myRecord struct {
    ID      []byte   `json:"-"`
    Name    string   `json:"name"`
}

func (r *myRecord) FdbxID() []byte               { return r.ID }
func (r *myRecord) FdbxType() uint16             { return MyFirstCollection }
func (r *myRecord) FdbxMarshal() ([]byte, error) { return json.Marshal(r) }
func (r *myRecord) FdbxUnmarshal(b []byte) error { return json.Unmarshal(b, r) }
```

### Create connection and drop database

```golang

// we use database number instead of string name 
const MyFirstDatabase = uint16(1)

// Create connection to your database
conn, err := fdbx.NewConn(MyFirstDatabase, 0)
if err != nil {
    return err
}

// Clear all database data
if err = conn.ClearDB(); err != nil {
    return err
}
```

### Save and load models

```golang

// Create some test object
recordToSave := &myRecord{
    ID: []byte("myID"), 
    Name: "my first record",
}

// Create transaction and save record
if err = conn.Tx(func(db fdbx.DB) (e error) { 
    return db.Save(recordToSave)
}); err != nil {
    return err
}

// .... many hours later ...

// Create another object
recordToLoad := &myRecord{ID: []byte("myID")}

// Fill it back
if err = conn.Tx(func(db fdbx.DB) (e error) { 
    return db.Load(recordToLoad)
}); err != nil {
    return err
}

// now, recordToSave.Name == recordToLoad.Name
```

### Select all collection

```golang
// we must specify Record Fabric to allow Cursor create objects
func recordFabric(id []byte) (fdbx.Record, error) { return &testRecord{ID: id}, nil }

// Create cursor for seq scan in batches of 10 records
cur, err := conn.Cursor(MyFirstCollection, recordFabric, nil, 10)
if err != nil {
    return err
}
defer cur.Close()

// return record and error channels
recChan, errChan := cur.Select(ctx, filter)

for record := range recChan {
    // some processing ...
}

for err := range errChan {
    if err != nil {
        return err
    }
}

```

### Publish record to queue and subscribe for it

```golang

queue, err := conn.Queue(MyQueueType, recordFabric, nil)
if err != nil {
    return err
}

conn.Tx(func(db fdbx.DB) (err error) {
    // Save record to database
    if err = db.Save(record); err != nil {
        return err
    }

    // Publish record in queue MyQueueType
    // This task processing will be delayed for 1 minute
    return queue.Pub(db, record.ID, time.Now().Add(time.Minute))
})

```

At another machine:

```golang

queue, err := conn.Queue(MyQueueType, recordFabric, nil)
if err != nil {
    return err
}

// return record and error channels
recChan, errChan := queue.Sub(ctx)

for record := range recChan {
    // some processing of queue items...

    // we chould ack processed task  
    conn.Tx(func(db fdbx.DB) (err error) {
        return queue.Ack(db, record.ID)
    })

}

for err := range errChan {
    if err != nil {
        return err
    }
}

```
