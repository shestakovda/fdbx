.PHONY: all fmt test

all: test

fmt:
	@goimports -w .
	@gofmt -s -w .
	@go mod tidy

test: fmt
	@flatc --go ./models_test.fbs
	@go test -count=10 -timeout 10s -race -cover ./...

bench: fmt
	@go test -bench . -benchmem -benchtime 30s .

lint:
	@golangci-lint run --enable-all --fix --tests=false