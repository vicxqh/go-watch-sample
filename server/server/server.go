package server

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/xiaowei1235/go-watch-sample/pb"
)

type Server struct {
	sync.RWMutex
	dbA map[string]string
	dbB map[string]string
	dbC map[string]string

	watchId int64
	events  chan *pb.Event

	sub   chan chan *pb.Event
	unsub chan chan *pb.Event
	subs  map[chan *pb.Event]struct{}
}

func NewServer() *Server {
	s := &Server{
		events: make(chan *pb.Event, 1),
		dbA:    make(map[string]string),
		dbB:    make(map[string]string),
		dbC:    make(map[string]string),
		sub:    make(chan chan *pb.Event),
		unsub:  make(chan chan *pb.Event),
		subs:   make(map[chan *pb.Event]struct{}),
	}
	go s.run()
	return s
}

func (s *Server) mockEvents() {
	events := []pb.EventType{pb.EventType_Delete, pb.EventType_Creat, pb.EventType_Update}
	resouces := []pb.ResourceType{pb.ResourceType_A, pb.ResourceType_B, pb.ResourceType_C}

	randStr := func() string {
		const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		b := make([]byte, 4)
		for i := range b {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		return string(b)
	}
	for range time.Tick(time.Second * 5) {
		e := &pb.Event{
			EType:    events[rand.Intn(3)],
			RType:    resouces[rand.Intn(3)],
			Value:    randStr(),
			OldValue: randStr(),
		}
		log.Printf("mock event %#v", e)
		s.events <- e
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
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	var db map[string]string
	switch req.Rtype {
	case pb.ResourceType_A:
		db = s.dbA
	case pb.ResourceType_B:
		db = s.dbB
	case pb.ResourceType_C:
		db = s.dbC
	default:
		return nil, errors.New("no such db")
	}

	k, v := req.Key, req.Value
	s.Lock()
	ov, existed := db[k]
	db[k] = v
	s.Unlock()
	tyype := pb.EventType_Creat
	if existed {
		tyype = pb.EventType_Update
	}
	s.events <- &pb.Event{
		EType:    tyype,
		RType:    req.Rtype,
		Value:    k + ", " + v,
		OldValue: k + ", " + ov,
	}
	return &pb.PutResponse{Update: existed}, nil
}
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	var db map[string]string
	switch req.Rtype {
	case pb.ResourceType_A:
		db = s.dbA
	case pb.ResourceType_B:
		db = s.dbB
	case pb.ResourceType_C:
		db = s.dbC
	default:
		return nil, errors.New("no such db")
	}

	k := req.Key
	s.Lock()
	ov, existed := db[k]
	delete(db, k)
	s.Unlock()
	if existed {
		s.events <- &pb.Event{
			EType:    pb.EventType_Delete,
			RType:    req.Rtype,
			Value:    k + ", ",
			OldValue: k + ", " + ov,
		}
	}
	return &pb.DeleteResponse{
		Removed: existed,
	}, nil
}

func (s *Server) Watch(stream pb.Watch_WatchServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	ws := &watchStream{
		watchers: make(map[int64]*watcher),
		stream:   stream,
		watchCh:  make(chan *pb.WatchResponse, 1024),
		eventCh:  make(chan *pb.Event, 1),
		ctx:      ctx,
		cancel:   cancel,
	}

	// post to stopc => terminate server stream; can't use a waitgroup
	// since all goroutines will only terminate after Watch() exits.
	stopc := make(chan struct{}, 3)
	go func() {
		defer func() { stopc <- struct{}{} }()
		ws.recvLoop()
	}()
	go func() {
		defer func() { stopc <- struct{}{} }()
		ws.sendLoop()
	}()

	go func() {
		defer func() { stopc <- struct{}{} }()
		ws.eventLoop()
	}()

	s.sub <- ws.eventCh
	defer func() {
		s.unsub <- ws.eventCh
	}()

	<-stopc
	cancel()
	return ctx.Err()
}

// watcher represents a watcher client
type watcher struct {
	req pb.WatchCreateRequest
	id  int64

	filter *filter

	// w is the parent.
	ws *watchStream
}

type watchStream struct {
	// mu protects watchers and nextWatcherID
	mu sync.Mutex
	// watchers receive events from watch broadcast.
	watchers map[int64]*watcher
	// nextWatcherID is the id to assign the next watcher on this stream.
	nextWatcherID int64

	stream pb.Watch_WatchServer

	// watchCh receives watch responses from the watchers.
	watchCh chan *pb.WatchResponse

	// eventCh receives all events from system.
	eventCh chan *pb.Event

	ctx    context.Context
	cancel context.CancelFunc
}

func (ws *watchStream) delete(id int64) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	delete(ws.watchers, id)
}

type filter struct {
	Resource   pb.ResourceType
	EventTypes []pb.EventType
}

func (f *filter) match(event *pb.Event) bool {
	if event.RType != f.Resource {
		return false
	}
	for i := range f.EventTypes {
		if f.EventTypes[i] == event.EType {
			return true
		}
	}
	return false
}

func filterFromRequest(req *pb.WatchCreateRequest) *filter {
	et := req.EType
	//TODO: remove duplicates
	if len(et) == 0 {
		et = []pb.EventType{pb.EventType_Creat, pb.EventType_Update, pb.EventType_Delete}
	}
	return &filter{
		Resource:   req.RType,
		EventTypes: et,
	}
}

func (ws *watchStream) recvLoop() error {
	for {
		req, err := ws.stream.Recv()
		if err != nil {
			return err
		}
		switch uv := req.RequestUnion.(type) {
		case *pb.WatchRequest_CreateRequest:
			cr := uv.CreateRequest

			w := &watcher{
				id:     ws.nextWatcherID,
				ws:     ws,
				filter: filterFromRequest(cr),
			}
			ws.nextWatcherID++
			ws.watchers[w.id] = w
			ws.watchCh <- &pb.WatchResponse{
				WatchId: w.id,
				Created: true,
			}
		case *pb.WatchRequest_CancelRequest:
			ws.delete(uv.CancelRequest.WatchId)
		default:
			panic("not implemented")
		}
	}
}

func (ws *watchStream) sendLoop() {
	for {
		select {
		case wresp, ok := <-ws.watchCh:
			if !ok {
				return
			}
			if err := ws.stream.Send(wresp); err != nil {
				return
			}
		case <-ws.ctx.Done():
			return
		}
	}
}

func (ws *watchStream) eventLoop() {
	for {
		select {
		case event, ok := <-ws.eventCh:
			if !ok {
				return
			}
			for id, w := range ws.watchers {
				if w.filter.match(event) {
					ws.watchCh <- &pb.WatchResponse{
						WatchId: id,
						Event:   event,
					}
				}
			}
		case <-ws.ctx.Done():
			return
		}
	}
}
