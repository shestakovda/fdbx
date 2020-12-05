# FoundationDB Extended v2

*STATUS: **beta**. Project in active development*

## About

Provides Queues and Indexed Tables with ORM-style queries for key-value objects

* `db.Connection` for "physical" fdb transactions
* `mvcc.Tx` for "logical" MVCC transactions
* `orm.Table` for storing all you data
* `orm.Queue` for planning all your tasks

## Usage

```sh
> go get "github.com/shestakovda/fdbx/v2"
```

```golang

import (
    "github.com/shestakovda/fdbx/v2"
    "github.com/shestakovda/fdbx/v2/db"
    "github.com/shestakovda/fdbx/v2/mvcc"
    "github.com/shestakovda/fdbx/v2/orm"
)

const DatabaseID byte = 0xDB
const DataTableID uint16 = 0x1234

// Define a collection
var DataTable = orm.NewTable(DataTableID)

// Create a connection
var conn db.Connection

if conn, err = db.ConnectV610(DatabaseID); err != nil {
    return
}

// Make a transaction
var tx mvcc.Tx

if tx, err = mvcc.Begin(conn); err != nil {
    return
}
defer tx.Cancel()

// Make some data
pair := fdbx.NewPair(
    fdbx.String2Key("test key"), 
    []byte("test value"),
)

// Insert some data
if err = DataTable.Upsert(tx, pair); err != nil {
    return
}

// Get it back
var selected fdbx.Pair

if selected, err = DataTable.Select(tx).First(); err != nil {
    return
}

// Commit transaction
if err = tx.Commit(); err != nil {
    return
}
```

## Tips and tricks

* Do not call `Commit` if it is read-only transaction: it will save you around 30 bytes of disk per call