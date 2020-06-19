package sdk

import (
	"context"
	"fmt"
	"os"

	"github.com/xiaowei1235/go-watch-sample/pb"

	"google.golang.org/grpc"
)

type Client struct {
	conn     *grpc.ClientConn
	callOpts []grpc.CallOption
	kv       pb.KVServiceClient
}

func (c *Client) Put(rtype pb.ResourceType, key, value string) error {
	_, err := c.kv.Put(context.TODO(), &pb.PutRequest{
		Rtype: rtype,
		Key:   key,
		Value: value,
	})
	return err
}

func (c *Client) Delete(rtype pb.ResourceType, key string) error {
	_, err := c.kv.Delete(context.TODO(), &pb.DeleteRequest{
		Rtype: rtype,
		Key:   key,
	})
	return err
}

func NewClient() *Client {
	server := "127.0.0.1:8080"
	conn, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("failed to connect server %s, %v\n", server, err)
		os.Exit(1)
	}
	return &Client{
		conn: conn,
		kv:   pb.NewKVServiceClient(conn),
	}
}
