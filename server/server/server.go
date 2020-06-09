package server

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/xiaowei1235/go-watch-sample/pb"
)

type Server struct {
	sync.RWMutex
	db     map[string]string
	events chan pb.Event

	sub   chan chan pb.Event
	unsub chan chan pb.Event
	subs  map[chan pb.Event]struct{}

	bench chan pb.BenchmarkRequest_Command
}

func NewServer() *Server {
	s := &Server{
		db:     make(map[string]string),
		events: make(chan pb.Event, 1),

		sub:   make(chan chan pb.Event),
		unsub: make(chan chan pb.Event),
		subs:  make(map[chan pb.Event]struct{}),

		bench: make(chan pb.BenchmarkRequest_Command),
	}
	go s.run()
	go s.benchmark()
	return s
}

func (s *Server) benchmark() {
	started := false

	var startTime, endTime time.Time
	var issued int

	p := func(e time.Time) {
		log.Printf("issued %d, qps: %f", issued, float64(issued)/e.Sub(startTime).Seconds())
	}

	randString := func() string {
		const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		b := make([]byte, 128)
		for i := range b {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		return string(b)
	}
	randEvent := func() pb.Event {
		return pb.Event{
			Type:     pb.Event_EventType(rand.Intn(3)),
			Key:      randString(),
			Value:    randString(),
			OldValue: randString(),
		}
	}

	for {
		select {
		case cmd := <-s.bench:
			switch cmd {
			case pb.BenchmarkRequest_start:
				started = true
				startTime = time.Now()
			case pb.BenchmarkRequest_stop:
				started = false
				endTime = time.Now()
				p(endTime)
				return
			case pb.BenchmarkRequest_stats:
				p(time.Now())
			}
		default:
			if started {
				s.events <- randEvent()
				issued++
			}
		}
	}
}

func (s *Server) run() {
	for {
		select {
		// subscribe a new watcher
		case l := <-s.sub:
			s.subs[l] = struct{}{}
			log.Printf("watcher count %d", len(s.subs))
		// unsubscribe a watcher
		case l := <-s.unsub:
			delete(s.subs, l)
			log.Printf("watcher count %d", len(s.subs))
		// broadcast new events to all watchers
		case e := <-s.events:
			for c, _ := range s.subs {
				c <- e
			}
		}
	}
}

func (s *Server) Put(ctx context.Context, request *pb.PutRequest) (*pb.PutResponse, error) {
	k, v := request.Key, request.Value
	log.Printf("put (%s, %s)", k, v)
	s.Lock()
	ov, existed := s.db[k]
	s.db[k] = v
	s.Unlock()
	tyype := pb.Event_Creat
	if existed {
		tyype = pb.Event_Update
	}
	s.events <- pb.Event{
		Type:     tyype,
		Key:      k,
		Value:    v,
		OldValue: ov,
	}
	return &pb.PutResponse{Update: existed}, nil
}

func (s *Server) Delete(ctx context.Context, request *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	k := request.Key
	log.Printf("delete %s", k)
	s.Lock()
	ov, existed := s.db[k]
	delete(s.db, k)
	s.Unlock()
	if existed {
		s.events <- pb.Event{
			Type:     pb.Event_Delete,
			Key:      k,
			Value:    "",
			OldValue: ov,
		}
	}
	return &pb.DeleteResponse{
		Removed: existed,
	}, nil
}

func (s *Server) Watch(filter *pb.Filter, stream pb.KVService_WatchServer) error {
	log.Printf("watcher %#v", stream)
	mchan := make(chan pb.Event, 1)
	s.sub <- mchan
	defer func() {
		s.unsub <- mchan
	}()
	for e := range mchan {
		// detect dead stream
		//https://github.com/improbable-eng/grpc-web/issues/57
		if err := stream.Context().Err(); err != nil {
			return err
		}

		send := false
		switch e.Type {
		case pb.Event_Delete:
			send = filter.Delete
		case pb.Event_Update:
			send = filter.Update
		case pb.Event_Creat:
			send = filter.Create
		}
		if send {
			err := stream.Send(&e)
			if err != nil {
				log.Printf("failed to send %+v to client, %v", e, err)
			}
		}
	}
	return nil
}

func (s *Server) WatchBenchmark(ctx context.Context, request *pb.BenchmarkRequest) (*pb.BenchmarkResponse, error) {
	s.bench <- request.Command
	return &pb.BenchmarkResponse{}, nil
}
