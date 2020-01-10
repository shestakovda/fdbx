.PHONY: all fmt test

all: test

fmt:
	@goimports -w .
	@gofmt -s -w .
	@go mod tidy

test: fmt
	@flatc --go ./models_test.fbs
	@go test -count=1 -race -cover ./...

bench: fmt
	@go test -bench BenchmarkLoad1000_1 -benchmem -benchtime 30s -memprofile mem.out -cpuprofile cpu.out .

lint:
	@golangci-lint run --enable-all --fix --tests=false