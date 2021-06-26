
migrate:
	go build -trimpath ./cmd/migrate

clean:
	rm migrate

.PHONY: update
update:
	$(GOENV) go get -u github.com/oligot/go-mod-upgrade
	$(GOENV) go-mod-upgrade
	$(GOENV) go mod tidy
	$(GOENV) go mod verify

.PHONY: test
test:
	go test ./...
