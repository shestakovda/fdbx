.PHONY: all fmt test

all: test

models.fbs:
	@flatc --go --gen-mutable --gen-object-api ./models.fbs

fmt: models.fbs
	@goimports -w .
	@go mod tidy

test: fmt
	@go test -count=10 -timeout 60s -run . -race -cover ./...

bench: fmt
	@go test -bench . -benchmem -benchtime 30s .

lint:
	@golangci-lint run --enable-all --fix --tests=false

mvcc: fmt
	@go test -count=1 -timeout 60s -run MVCC -gcflags=all=-d=checkptr=0 -race -cover -coverprofile=./mvcc.cover ./mvcc

mvcc-cover:
	@go tool cover -html=./mvcc.cover

mvcc-bench: fmt
	@go test -bench=. -benchmem -benchtime 30s ./mvcc

orm: fmt
	@go test -count=1 -timeout 60s -run ORM -gcflags=all=-d=checkptr=0 -race -cover -coverprofile=./orm.cover ./orm
