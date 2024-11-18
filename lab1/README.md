# Resources

All resources are located in the [public](./public) directory.
Unless otherwise specified, all resources are served from this directory, and stored there as well.

# Using Makefile

You can find the Makefile [here](./Makefile).

- `make` to build all binaries.
- `make test` to run all tests.
- `make run-http port=<port>` to run the server.
- `make run-proxy port=<port>` to run the proxy.

- `make clean` to remove all binaries.
- `make clean-pub` to remove all user-created files in the public directory.

# Using Docker

The http-server can also be run using Docker (compose)

- `docker-compose up` to run the server. It will use port 80.

# Manually

## Http Server

```
go build -o bin/http_server ./cmd/httpserver
```

## Proxy

```
go build -o bin/proxy ./cmd/proxy
```

# Usage

## HttpServer

```
./bin/http_server <port>
```

## Proxy

```
./bin/proxy <port>
```

# Test

## Run all tests

```
go test ./...
```
