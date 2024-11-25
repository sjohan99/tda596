.PHONY: build coord worker build-no-race coord-no-race worker-no-race

all: clean build coord

build:
	go build -race -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go

coord:
	go run -race mrcoordinator.go pg-*.txt

worker:
	go run -race -gcflags="all=-N -l" mrworker.go wc.so

build-no-race:
	go build -race -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go

coord-no-race:
	go run -race mrcoordinator.go pg-*.txt

worker-no-race:
	go run -race -gcflags="all=-N -l" mrworker.go wc.so

clean:
	rm -f mr-*