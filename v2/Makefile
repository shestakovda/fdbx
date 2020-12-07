.PHONY: fdbx db mvcc orm

TESTARGS=-run . -timeout 600s -failfast -race -gcflags=all=-d=checkptr=0 -count=30 -cover
BENCHARGS=-bench . -benchtime=3s -benchmem

all: test

clean:
	@rm -rf ./models
	@rm -rf ./bin

fmt: clean
	@mkdir -p ./bin
	@flatc --go --gen-mutable --gen-object-api ./models.fbs
	@goimports -w .
	@go mod tidy

test: fdbx db mvcc orm

fdbx: fmt
	@go test $(TESTARGS) -o=./bin/fdbx.test -coverprofile=./bin/fdbx.cover .

db: fmt
	@go test $(TESTARGS) -o=./bin/db.test -coverprofile=./bin/db.cover ./db

mvcc: fmt
	@go test $(TESTARGS) -o=./bin/mvcc.test -coverprofile=./bin/mvcc.cover ./mvcc

orm: fmt
	@go test $(TESTARGS) -o=./bin/orm.test -coverprofile=./bin/orm.cover ./orm

bench: bench-fdbx bench-mvcc bench-orm

bench-fdbx:
	@go test $(BENCHARGS) -o=./bin/fdbx.test -memprofile=./bin/fdbx.mem .

bench-mvcc:
	@go test $(BENCHARGS) -o=./bin/mvcc.test -memprofile=./bin/mvcc.mem ./mvcc

bench-orm:
	@go test $(BENCHARGS) -o=./bin/orm.test -memprofile=./bin/orm.mem ./orm

cover: 
# cover: test
# 	@go tool cover -html=./bin/fdbx.cover
# 	@go tool cover -html=./bin/db.cover
# 	@go tool cover -html=./bin/mvcc.cover
	@go tool cover -html=./bin/orm.cover