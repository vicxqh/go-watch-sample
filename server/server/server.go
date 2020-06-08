package server

import (
	"context"
	"log"
	"sync"

	"github.com/xiaowei1235/go-watch-sample/pb"
)

type Server struct {
	sync.RWMutex
	db     map[string]string
	events chan pb.Event

	sub   chan chan pb.Event
	unsub chan chan pb.Event
	subs  map[chan pb.Event]struct{}
}

func NewServer() *Server {
	s := &Server{
		db:     make(map[string]string),
		events: make(chan pb.Event, 1),

		sub:   make(chan chan pb.Event),
		unsub: make(chan chan pb.Event),
		subs:  make(map[chan pb.Event]struct{}),
	}
	go s.run()
	return s
}

func (s *Server) run() {
	for {
		select {
		// subscribe a new watcher
		case l := <-s.sub:
			s.Lock()
			s.subs[l] = struct{}{}
			log.Printf("watcher count %d", len(s.subs))
			s.Unlock()
		// unsubscribe a watcher
		case l := <-s.unsub:
			s.Lock()
			delete(s.subs, l)
			log.Printf("watcher count %d", len(s.subs))
			s.Unlock()
		// broadcast new events to all wathers
		case e := <-s.events:
			s.RLock()
			ls := make([]chan pb.Event, 0, len(s.subs))
			for c, _ := range s.subs {
				ls = append(ls, c)
			}
			s.RUnlock()
			go func() {
				for _, l := range ls {
					l <- e
				}
			}()
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
