
GIT_SUMMARY := $(shell git describe --tags --dirty --always)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
BUILD_STAMP := $(shell date -u '+%Y-%m-%dT%H:%M')
REVISION := $(GIT_SUMMARY)-$(GIT_BRANCH)-$(BUILD_STAMP)

lint:
	golangci-lint run --enable-all -D=lll,gochecknoglobals,gosec,funlen,wsl,gochecknoinits

test:
	go test -v -race -cover ./...

build:
	CGO_ENABLED=0 go build -ldflags '-s -extldflags "-static" -X "main.revision=$(REVISION)"' -o proximo-server .
