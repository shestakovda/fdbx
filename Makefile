all: test

models:
	@flatc --go --gen-mutable --gen-object-api ./models.fbs

fmt: models
	@goimports -w .
	@go mod tidy

test: test-fdbx test-db test-mvcc test-orm

test-fdbx: fmt
	@go test -timeout 10s -run . -race -gcflags=all=-d=checkptr=0 -count=1 -cover -coverprofile=./fdbx.cover .

test-db: fmt
	@go test -timeout 10s -run . -race -gcflags=all=-d=checkptr=0 -count=1 -cover -coverprofile=./db.cover ./db

test-mvcc: fmt
	@go test -timeout 60s -run . -race -gcflags=all=-d=checkptr=0 -count=1 -cover -coverprofile=./mvcc.cover ./mvcc

test-orm: fmt
	@go test -timeout 10s -run . -race -gcflags=all=-d=checkptr=0 -count=1 -cover -coverprofile=./orm.cover ./orm

bench: bench-fdbx bench-mvcc bench-orm

bench-fdbx: fmt
	@go test -bench . -benchtime=3s -benchmem -memprofile=fdbx.mem .

bench-mvcc: fmt
	@go test -bench . -benchtime=3s -benchmem -memprofile=mvcc.mem ./mvcc
	
bench-orm: fmt
	@go test -bench . -benchtime=3s -benchmem -memprofile=orm.mem ./orm

cover: test
	@go tool cover -html=./fdbx.cover