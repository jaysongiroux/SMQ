# Makefile for SMQ

# Default target
default: build

# Build the binary
build:
	go build -o bin/smq main.go

# Run the binary
run: build
	./bin/smq

# Clean up
clean:
	rm -f bin/smq

# run tests
test:
	go test ./...

# run tests with coverage
test-coverage:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out	
	
lint:
	golangci-lint run --fix ./...
