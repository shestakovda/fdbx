TESTARGS=-run . -failfast -race -gcflags=all=-d=checkptr=0 -count=1 -cover

all: test

fbs:
	@flatc --go --gen-mutable --gen-object-api ./models.fbs

fmt: fbs
	@goimports -w .
	@go mod tidy

test: test-fdbx test-db test-mvcc test-orm

test-fdbx: fmt
	@go test -timeout 10s -coverprofile=./bin/fdbx.cover .

test-db: fmt
	@go test -timeout 10s -coverprofile=./bin/db.cover ./db

test-mvcc: fmt
	@go test -timeout 30s -coverprofile=./bin/mvcc.cover ./mvcc

test-orm: fmt
	@go test -timeout 10s -coverprofile=./bin/orm.cover ./orm

bench: bench-fdbx bench-mvcc bench-orm

bench-fdbx:
	@go test -bench . -benchtime=3s -benchmem -memprofile=./bin/fdbx.mem .

bench-mvcc:
	@go test -bench . -benchtime=3s -benchmem -memprofile=./bin/mvcc.mem ./mvcc

bench-orm:
	@go test -bench . -benchtime=3s -benchmem -memprofile=./bin/orm.mem ./orm

cover: test
	@go tool cover -html=./bin/fdbx.cover
	@go tool cover -html=./bin/db.cover
	@go tool cover -html=./bin/mvcc.cover
	@go tool cover -html=./bin/orm.cover