package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/xiaowei1235/go-watch-sample/pb"
	"github.com/xiaowei1235/go-watch-sample/server/server"
	"google.golang.org/grpc"
)

var port int

func init() {
	flag.IntVar(&port, "port", 8080, "server port")
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	log.Printf("listened on port %d", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	server := server.NewServer()
	pb.RegisterWatchServer(grpcServer, server)
	pb.RegisterKVServiceServer(grpcServer, server)
	grpcServer.Serve(lis)
}
