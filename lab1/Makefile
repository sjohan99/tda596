.PHONY: all build-http build-proxy run-http run-proxy test clean clean-pub 

all: build-http build-proxy

build-http:
	go build -o bin/http_server ./cmd/httpserver

build-proxy:
	go build -o bin/proxy ./cmd/proxy

run-http:
	./bin/http_server $(port)

run-proxy:
	./bin/proxy $(port)

test:
	go test ./...

clean:
	rm -f bin/http_server bin/proxy

clean-pub:
	find public -type f ! -name 'image.png' ! -name 'testfile.txt' ! -name 'index.html' ! -name 'cat.jpg' ! -name 'spongebob.gif' ! -name 'index.css' -delete
