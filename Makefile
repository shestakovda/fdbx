.PHONY: all fmt test

all: test

fmt:
	@goimports -w .
	@gofmt -s -w .

test: fmt
	@go test -race -cover ./...

lint:
	@golangci-lint run --enable-all --fix