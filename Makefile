.PHONY: build test test-integration lint vet docker clean

BINARY := nats-provisioner

build:
	go build -o $(BINARY) ./cmd/nats-provisioner

test:
	go test -race ./...

test-integration:
	go test -race -tags=integration -v ./...

lint:
	golangci-lint run

vet:
	go vet ./...

docker:
	docker build -t $(BINARY) .

clean:
	rm -f $(BINARY)
