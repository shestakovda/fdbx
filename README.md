## FoundationDB eXtended

[![GoDoc](https://godoc.org/github.com/shestakovda/fdbx?status.svg)](https://godoc.org/github.com/shestakovda/fdbx)
[![Codebeat](https://codebeat.co/badges/6909b169-393c-4c2b-aa9c-b9b1c3ff5708)](https://codebeat.co/projects/github-com-shestakovda-fdbx-master)
[![Go Report Card](https://goreportcard.com/badge/github.com/shestakovda/fdbx)](https://goreportcard.com/report/github.com/shestakovda/fdbx)

FoundationDB **object storage** and **queue manager** for Golang projects

### About

This project aims to help you use FoundationDB in your project by providing high-level interfaces for transactions, tables, indexes and queues. 

Current version is **unstable** [v2.0.0-prerelease.*](https://github.com/shestakovda/fdbx/tree/master/v2)

You can carefully use it for local tests, experiments or startups, but we strongly recommend not to use it for production purposes. 

### Basic principles

* MVCC logical transactions above the standard fdb "physical" transactions
* All data is stored as key-value Pairs in Tables (collections with key prefix)
* Queries is an orm-style set of simple data operators for Table manipulations
* Indexes is a subcollection of Table with value-id rows 
* Queues is a special indexes with time-series data and exclusive reads

### Features

* You can store **objects of *(almost)* any size**: we overcome size limitations!
    - Standard fdb values [has 100Kb limit](https://apple.github.io/foundationdb/known-limitations.html#large-keys-and-values), but our Pairs has not
    - Standard fdb transaction [has 10Mb limit](https://apple.github.io/foundationdb/known-limitations.html#large-transactions), but our logical transactions has not
* You can **process data as long as you need**: we overcome time limitations!
    - Standard fdb transaction [has 5 sec limit](https://apple.github.io/foundationdb/known-limitations.html#long-running-transactions), but our logical transactions has not
* You can **use transactional queues**: synchronize it with your data
    - Pub, Ack and Repeat changes are applying at a commit time
    - If you cancel a transaction, no tasks would be published or acknowledged
* You can **store any data you want**: any value is just a byte slice
    - Table and Queue interfaces are schemaless
    - Indexes are optional and schemaless
    - You can use raw data, JSON, XML, FlatBuffers, Protobuf or anything else you want

### Disadvantages

* Standard fdb **serializable** isolation level is downgraded to **read committed** because of MVCC
    - You can use `SharedLock` function to avoid concurrent writes
    - We can support *repeateable read* in the future, if needed
* Big values (over 100Kb data) reads has some overhead, and they are significant when the data is over 100Mb
    - Overhead is significant compared with raw file reads
    - You can use gzip or smth else to compress data before saving
* Total read/write throughput (objects/sec) are downgraded because of transactions and replication overhead
    - It less then other MVCC systems, like PostgreSQL, but we can better scale because of FoundationDB replication
    - It less then other key-value systems, like Redis or MongoDB, but we can use a transactions (almost) without a limitations
* Total queue throughput (tasks/sec) are downgraded because of transactions and replication overhead
    - It less then other queue managers, like a RabbitMQ or NATS, but we has a transaction-reliable delayable ordered tasks without duplicates

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

## Thanks

Inspired from [Apple's FoundationDB Record Layer](https://arxiv.org/pdf/1901.04452.pdf)

Powered by [Apple's FoundationDB Golang bindings](https://godoc.org/github.com/apple/foundationdb/bindings/go/src/fdb)

Supercharged by [FlatBuffers](http://google.github.io/flatbuffers/index.html)