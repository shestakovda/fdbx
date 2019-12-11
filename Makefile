.PHONY: all fmt test

all: test

fmt:
	@goimports -w .
	@gofmt -s -w .

test: fmt
	@go test -count=10 -race -cover ./...

bench: fmt
	@go test -bench . -benchmem -benchtime 10s ./...

lint:
	@golangci-lint run --enable-all --fix --tests=false