
test:
	docker-compose run migrate ./_scripts/test.sh | sed 's/\r$\//'

PHONY: test
