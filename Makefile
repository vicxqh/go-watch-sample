all : server client
proto :
	@echo "building proto"
	protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative ./pb/service.proto

server :
	@echo "building server"
	go build -o bin/server ./server

client :
	@echo "building client"
	go build -o bin/client ./client

.PHONY: proto server client

