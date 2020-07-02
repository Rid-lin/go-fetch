build: buildwothoutdebug_linux pack

all: buildwothoutdebug buildwothoutdebug_linux pack

buildfordebug:
	go build -o build/go-fetch -v ./

buildwothoutdebug_linux:
	set GOOS=linux&& go build --ldflags "-w -s" -o build/go-fetch -v ./

run: build
	build/go-fetch
	
.DUFAULT_GOAL := build

pack:
	upx --ultra-brute build\go-fetch*

mod:
	go mod tidy
	go mod download
	go mod vendor