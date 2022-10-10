
migrate:
	go build -trimpath ./cmd/migrate

.PHONY: clean
clean:
	rm migrate

.PHONY: update
update:
	$(GOENV) go get github.com/oligot/go-mod-upgrade
	$(GOENV) go run github.com/oligot/go-mod-upgrade
	$(GOENV) go mod tidy
	$(GOENV) go mod verify

.PHONY: test
test:
	go test -v ./...
