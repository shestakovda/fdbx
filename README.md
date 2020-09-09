## FoundationDB eXtended

[![GoDoc](https://godoc.org/github.com/shestakovda/fdbx?status.svg)](https://godoc.org/github.com/shestakovda/fdbx)
[![Codebeat](https://codebeat.co/badges/6909b169-393c-4c2b-aa9c-b9b1c3ff5708)](https://codebeat.co/projects/github-com-shestakovda-fdbx-master)
[![Go Report Card](https://goreportcard.com/badge/github.com/shestakovda/fdbx)](https://goreportcard.com/report/github.com/shestakovda/fdbx)

FoundationDB **object storage** and **queue manager** for golang projects

**Project in active development! Production-fatality**

### About

This project aims to help you use FoundationDB in your project by providing high-level interfaces for storing objects. 

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

## Contributing

**Feel free to create issues and PRs!**

### New to FoundationDB? 

You're lucky, it's awesome and there are good [overview](https://apple.github.io/foundationdb/index.html) for it

### Okay, you wanna to improve some....

... and first of all you must [install FoundationDB](https://www.foundationdb.org/download/)!

Remember that you should make some tests before commit:

```sh
> make test
```

You can help this project to improve code style:

```sh
> make lint
```

## Thanks

Inspired from [Apple's FoundationDB Record Layer](https://arxiv.org/pdf/1901.04452.pdf)

Powered by [Apple's FoundationDB Golang bindings](https://godoc.org/github.com/apple/foundationdb/bindings/go/src/fdb)

Supercharged by [FlatBuffers](http://google.github.io/flatbuffers/index.html)