package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/xiaowei1235/go-watch-sample/pb"

	"google.golang.org/grpc"
)

var server string

func init() {
	flag.StringVar(&server, "server", "127.0.0.1:8080", "server port")
}

const helpMsg = `Help: 
	put key value
	del key
	watch [create,update,delete]   # e.g. watch create,delete`

var client pb.KVServiceClient

func main() {
	conn, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("failed to connect server %s, %v\n", server, err)
		os.Exit(1)
	}
	defer conn.Close()

	client = pb.NewKVServiceClient(conn)

	fmt.Print("> ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmd := scanner.Text()
		handleCmd(cmd)
		fmt.Print("> ")
	}
}

func handleCmd(cmd string) {
	tokens := strings.Split(cmd, " ")
	if len(tokens) == 0 {
		goto help_and_exit
	}

	switch tokens[0] {
	case "put":
		if len(tokens) != 3 {
			goto help_and_exit
		}
		res, err := client.Put(context.Background(), &pb.PutRequest{
			Key:   tokens[1],
			Value: tokens[2],
		})
		if err != nil {
			fmt.Println(fmt.Sprintf("failed to put, %v", err))
		}
		if res.Update {
			fmt.Println("updated")
		} else {
			fmt.Println("created")
		}
	case "del":
		if len(tokens) != 2 {
			goto help_and_exit
		}
		res, err := client.Delete(context.Background(), &pb.DeleteRequest{
			Key: tokens[1],
		})
		if err != nil {
			fmt.Println(fmt.Sprintf("failed to delete, %v", err))
		}
		if res.Removed {
			fmt.Println("removed")
		} else {
			fmt.Println("no such key")
		}
	case "benchstart":
		client.WatchBenchmark(context.Background(), &pb.BenchmarkRequest{
			Command: pb.BenchmarkRequest_start,
		})
	case "benchstop":
		client.WatchBenchmark(context.Background(), &pb.BenchmarkRequest{
			Command: pb.BenchmarkRequest_stop,
		})
	case "benchstats":
		client.WatchBenchmark(context.Background(), &pb.BenchmarkRequest{
			Command: pb.BenchmarkRequest_stats,
		})
	case "benchwatch":
		if len(tokens) != 2 {
			goto help_and_exit
		}
		option := tokens[1]
		opts := strings.Split(option, ",")
		filter := pb.Filter{}
		for _, opt := range opts {
			if opt == "create" {
				filter.Create = true
			} else if opt == "delete" {
				filter.Delete = true
			} else if opt == "update" {
				filter.Update = true
			} else {
				goto help_and_exit
			}
		}

		stream, err := client.Watch(context.Background(), &filter)
		if err != nil {
			fmt.Println(fmt.Sprintf("failed to watch, %v", err))
			return
		}
		started := false
		var startTime time.Time
		var received int
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("server aborted")
				return
			}
			if err != nil {
				fmt.Println(fmt.Sprintf("failed to receive event, %v", err))
				return
			}
			if started {
				received++
				if received%100 == 0 {
					fmt.Printf("received %d, qps: %.2f\n", received, float64(received)/time.Now().Sub(startTime).Seconds())
				}
			} else {
				started = true
				startTime = time.Now()
			}
		}

	case "watch":
		if len(tokens) != 2 {
			goto help_and_exit
		}
		option := tokens[1]
		opts := strings.Split(option, ",")
		filter := pb.Filter{}
		for _, opt := range opts {
			if opt == "create" {
				filter.Create = true
			} else if opt == "delete" {
				filter.Delete = true
			} else if opt == "update" {
				filter.Update = true
			} else {
				goto help_and_exit
			}
		}

		stream, err := client.Watch(context.Background(), &filter)
		if err != nil {
			fmt.Println(fmt.Sprintf("failed to watch, %v", err))
			return
		}
		fmt.Println("hit CTRL-C to exit watch")

		userSignal := make(chan os.Signal, 1)
		signal.Notify(userSignal, os.Interrupt)

		closing := make(chan struct{})
		errChan := make(chan struct{})
		go func() {
			defer func() {
				close(errChan)
			}()
			for {
				select {
				case <-closing:
					return
				default:
				}
				event, err := stream.Recv()
				if err == io.EOF {
					fmt.Println("server aborted")
					return
				}
				if err != nil {
					fmt.Println(fmt.Sprintf("failed to receive event, %v", err))
					return
				}
				printEvent(*event)
			}
		}()

		select {
		case <-errChan:
		case <-userSignal:
		}
		signal.Reset(os.Interrupt)
		close(closing)
	default:
		goto help_and_exit
	}

	return

help_and_exit:
	fmt.Println(helpMsg)
	return
}

func printEvent(event pb.Event) {
	switch event.Type {
	case pb.Event_Creat:
		fmt.Printf("Create (%s, %s)\n", event.Key, event.Value)
	case pb.Event_Update:
		fmt.Printf("Update (%s, %s -> %s)\n", event.Key, event.OldValue, event.Value)
	case pb.Event_Delete:
		fmt.Printf("Delete %s, %s(last value)\n", event.Key, event.OldValue)
	}
}
