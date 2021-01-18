PWD?=$(shell pwd)
APP?=$(shell basename $(PWD))
COMMIT?=$(shell git rev-parse --short HEAD)
VERSION?=$(shell git describe --tags)
BUILD_TIME?=$(shell date -u '+%Y-%m-%d_%H:%M:%S')

build: buildwothoutdebug_linux pack

all: buildwothoutdebug buildwothoutdebug_linux pack

buildfordebug:
	go build -o build/go-fetch -v ./

buildwothoutdebug_linux:
	set GOOS=linux&& go build --ldflags "-w -s" -o build/go-fetch -v ./

linux: 
	CGO_ENABLED=0 GOOS=linux   go build -v -mod vendor \
		-ldflags "-w -s -X main.Version=$(VERSION) -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
		-o bin/linux/${APP}
	upx bin/linux/${APP}

run: build
	build/go-fetch
	
.DUFAULT_GOAL := build

.PHONY: pack
pack:
	upx --ultra-brute build\go-fetch*

.PHONY: mod
mod:
	go mod tidy
	go mod download
	go mod vendor