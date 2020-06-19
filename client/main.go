package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/xiaowei1235/go-watch-sample/sdk"

	"github.com/xiaowei1235/go-watch-sample/pb"
)

const helpMsg = `Help: 
	put resourceType[a|b|c] key value
	del resourceType[a|b|c] key
	watch resourceType[a|b|c] EventType[create,update,delete]   # e.g. watch a create,delete`

var watcher sdk.Watcher
var client *sdk.Client

func main() {
	client = sdk.NewClient()
	watcher = sdk.NewWatcher(client)
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
	var rtype pb.ResourceType
	if len(tokens) == 0 {
		goto help_and_exit
	}
	cmd = tokens[0]
	tokens = tokens[1:]
	switch tokens[0] {
	case "a":
		rtype = pb.ResourceType_A
	case "b":
		rtype = pb.ResourceType_B
	case "c":
		rtype = pb.ResourceType_C
	}
	switch cmd {
	case "put":
		if len(tokens) != 3 {
			goto help_and_exit
		}
		err := client.Put(rtype, tokens[1], tokens[2])
		if err != nil {
			fmt.Println(fmt.Sprintf("failed to put, %v", err))
		}

	case "del":
		if len(tokens) != 2 {
			goto help_and_exit
		}
		err := client.Delete(rtype, tokens[1])
		if err != nil {
			fmt.Println(fmt.Sprintf("failed to delete, %v", err))
		}
	case "watch":
		ts := []pb.EventType{}
		var wc sdk.WatchChan
		if len(tokens) > 2 || len(tokens) == 0 {
			goto help_and_exit
		}

		if len(tokens) == 2 {
			tss := strings.Split(tokens[1], ",")
			for _, s := range tss {
				switch s {
				case "create":
					ts = append(ts, pb.EventType_Creat)
				case "delete":
					ts = append(ts, pb.EventType_Delete)
				case "update":
					ts = append(ts, pb.EventType_Update)
				}
			}
		}

		switch tokens[0] {
		case "a":
			wc = watcher.Watch(pb.ResourceType_A, ts...)
		case "b":
			wc = watcher.Watch(pb.ResourceType_B, ts...)
		case "c":
			wc = watcher.Watch(pb.ResourceType_C, ts...)

		default:
			goto help_and_exit
		}

		if wc != nil {
			go func() {
				for rsp := range wc {
					log.Printf("[client watch response] >> %s", sprintResponse(rsp))
				}
			}()
		}
	default:
		goto help_and_exit
	}
	return

help_and_exit:
	fmt.Println(helpMsg)
	return
}

func sprintResponse(rsp sdk.WatchResponse) string {
	return fmt.Sprintf("%#v", rsp.Event)
}
