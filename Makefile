.PHONY: all fmt test

all: test

fmt:
	@flatc --go ./models.fbs
	@goimports -w .
	@go mod tidy

test: fmt
	@go test -count=10 -timeout 10s -run . -race -cover ./...

bench: fmt
	@go test -bench . -benchmem -benchtime 30s .

lint:
	@golangci-lint run --enable-all --fix --tests=false