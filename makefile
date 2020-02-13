
migrate:
	go build -trimpath ./cmd/migrate

clean:
	rm migrate

.PHONY: test
test:
	go test ./...
