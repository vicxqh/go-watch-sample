package sdk

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/xiaowei1235/go-watch-sample/pb"

	"google.golang.org/grpc"
)

const maxBackoff = 100 * time.Millisecond

type Event pb.Event

type WatchResponse struct {
	Event *Event

	// Created is used to indicate the creation of the watcher.
	Created bool
}

type WatchChan <-chan WatchResponse

type Watcher interface {
	Watch(rtype pb.ResourceType, eType ...pb.EventType) WatchChan
}

// watcher implements the Watcher interface
type watcher struct {
	remote   pb.WatchClient
	callOpts []grpc.CallOption

	mu     sync.RWMutex
	stream *watchGrpcStream
}

// watchGrpcStream tracks all watch resources attached to a single grpc stream.
type watchGrpcStream struct {
	remote   pb.WatchClient
	callOpts []grpc.CallOption

	// ctx controls internal remote.Watch requests
	ctx context.Context

	// substreams holds all active watchers on this grpc stream
	substreams map[int64]*watcherStream
	// resuming holds all resuming watchers on this grpc stream
	resuming []*watcherStream

	// reqc sends a watch request from Watch() to the main goroutine
	reqc chan *watchRequest

	// respc receives data from the watch client
	respc chan *pb.WatchResponse

	// resumec closes to signal that all substreams should begin resuming
	resumec chan struct{}

	// wg is Done when all substream goroutines have exited
	wg sync.WaitGroup

	// errc transmits errors from grpc Recv to the watch stream reconnect logic
	errc chan error
}

// watcherStream represents a registered watcher
type watcherStream struct {
	// initReq is the request that initiated this request
	initReq watchRequest

	// outc publishes watch responses to subscriber
	outc chan WatchResponse
	// recvc buffers watch responses before publishing
	recvc chan *WatchResponse
	// donec closes when the watcherStream goroutine stops.
	donec chan struct{}

	// id is the registered watch id on the grpc stream
	id int64

	// buf holds all events received from server but not yet consumed by the client
	buf []*WatchResponse
}

type watchRequest struct {
	ctx      context.Context
	resource pb.ResourceType
	event    []pb.EventType

	// send created notification event if this field is true
	createdNotify bool

	// retc receives a chan WatchResponse once the watcher is established
	retc chan chan WatchResponse
}

func (w *watchRequest) toPB() *pb.WatchRequest {
	return &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{
			RType: w.resource,
			EType: w.event,
		},
	}}
}

func NewWatcher(c *Client) Watcher {
	return NewWatchFromWatchClient(pb.NewWatchClient(c.conn), c)
}

func NewWatchFromWatchClient(wc pb.WatchClient, c *Client) Watcher {
	w := &watcher{
		remote: wc,
	}
	if c != nil {
		w.callOpts = c.callOpts
	}
	return w
}

func (w *watcher) newWatcherGrpcStream() *watchGrpcStream {
	wgs := &watchGrpcStream{
		remote:     w.remote,
		callOpts:   w.callOpts,
		ctx:        context.Background(),
		substreams: make(map[int64]*watcherStream),
		respc:      make(chan *pb.WatchResponse),
		reqc:       make(chan *watchRequest),
		errc:       make(chan error, 1),
		resumec:    make(chan struct{}),
	}
	go wgs.run()
	return wgs
}

// openWatchClient retries opening a watch client until success
func (w *watchGrpcStream) openWatchClient() (ws pb.Watch_WatchClient, err error) {
	backoff := time.Millisecond
	for {
		select {
		case <-w.ctx.Done():
			if err == nil {
				return nil, w.ctx.Err()
			}
			return nil, err
		default:
		}
		if ws, err = w.remote.Watch(w.ctx, w.callOpts...); ws != nil && err == nil {
			break
		}
		if err != nil {
			log.Printf("Vic>> failed to open watch client, %v", err)
			// retry, but backoff
			if backoff < maxBackoff {
				// 25% backoff factor
				backoff = backoff + backoff/4
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			time.Sleep(backoff)
		}
	}
	return ws, nil
}

// serveSubstream forwards watch responses from run() to the subscriber
func (w *watchGrpcStream) serveSubstream(ws *watcherStream, resumec chan struct{}) {
	defer func() {
		close(ws.donec)
		w.wg.Done()
	}()

	emptyWr := &WatchResponse{}
	for {
		curWr := emptyWr
		outc := ws.outc

		if len(ws.buf) > 0 {
			curWr = ws.buf[0]
		} else {
			outc = nil
		}
		select {
		case outc <- *curWr:
			ws.buf[0] = nil
			ws.buf = ws.buf[1:]
		case wr, ok := <-ws.recvc:
			if !ok {
				// shutdown from closeSubstream
				return
			}

			if wr.Created {
				if ws.initReq.retc != nil {
					ws.initReq.retc <- ws.outc
					// to prevent next write from taking the slot in buffered channel
					// and posting duplicate create events
					ws.initReq.retc = nil

					// send first creation event only if requested
					if ws.initReq.createdNotify {
						ws.outc <- *wr
					}
				}
			}

			// created event is already sent above,
			// watcher should not post duplicate events
			if wr.Created {
				continue
			}

			// TODO pause channel if buffer gets too large
			ws.buf = append(ws.buf, wr)
		case <-w.ctx.Done():
			panic("watch cancellation not supported yet")
			return
		case <-ws.initReq.ctx.Done():
			panic("watch cancellation not supported yet")
			return
		case <-resumec:
			log.Printf("Vic>> substream %d resuming", ws.id)
			return
		}
	}
	// lazily send cancel message if events on missing id
}

// joinSubstreams waits for all substream goroutines to complete.
func (w *watchGrpcStream) joinSubstreams() {
	for _, ws := range w.substreams {
		<-ws.donec
	}
	for _, ws := range w.resuming {
		if ws != nil {
			<-ws.donec
		}
	}
}

// serveWatchClient forwards messages from the grpc stream to run()
func (w *watchGrpcStream) serveWatchClient(wc pb.Watch_WatchClient) {
	for {
		resp, err := wc.Recv()
		if err != nil {
			w.errc <- err
			return
		}
		w.respc <- resp
	}
}

func (w *watchGrpcStream) newWatchClient() (pb.Watch_WatchClient, error) {
	// mark all substreams as resuming
	close(w.resumec)
	w.resumec = make(chan struct{})
	w.joinSubstreams()
	for _, ws := range w.substreams {
		ws.id = -1
		w.resuming = append(w.resuming, ws)
	}
	// strip out nils, if any
	var resuming []*watcherStream
	for _, ws := range w.resuming {
		if ws != nil {
			resuming = append(resuming, ws)
		}
	}
	w.resuming = resuming
	w.substreams = make(map[int64]*watcherStream)

	// connect to grpc stream
	wc, err := w.openWatchClient()

	// serve all non-closing streams, even if there's a client error
	// so that the teardown path can shutdown the streams as expected.
	for _, ws := range w.resuming {
		ws.donec = make(chan struct{})
		w.wg.Add(1)
		go w.serveSubstream(ws, w.resumec)
	}

	if err != nil {
		return nil, err
	}

	// receive data from new grpc stream
	go w.serveWatchClient(wc)
	return wc, nil
}

func (w *watchGrpcStream) addSubstream(resp *pb.WatchResponse, ws *watcherStream) {
	ws.id = resp.WatchId
	w.substreams[ws.id] = ws
}

// dispatchEvent sends a WatchResponse to the appropriate watcher stream
func (w *watchGrpcStream) dispatchEvent(pbresp *pb.WatchResponse) bool {
	wr := &WatchResponse{
		Event:   (*Event)(pbresp.Event),
		Created: pbresp.Created,
	}

	return w.unicastResponse(wr, pbresp.WatchId)
}

// nextResume chooses the next resuming to register with the grpc stream. Abandoned
// streams are marked as nil in the queue since the head must wait for its inflight registration.
func (w *watchGrpcStream) nextResume() *watcherStream {
	for len(w.resuming) != 0 {
		if w.resuming[0] != nil {
			return w.resuming[0]
		}
		w.resuming = w.resuming[1:len(w.resuming)]
	}
	return nil
}

// unicastResponse sends a watch response to a specific watch substream.
func (w *watchGrpcStream) unicastResponse(wr *WatchResponse, watchId int64) bool {
	ws, ok := w.substreams[watchId]
	if !ok {
		return false
	}
	select {
	case ws.recvc <- wr:
	case <-ws.donec:
		return false
	}
	return true
}

// run is the root of the goroutines for managing a watcher client
func (w *watchGrpcStream) run() {
	var wc pb.Watch_WatchClient
	var closeErr error

	if wc, closeErr = w.newWatchClient(); closeErr != nil {
		return
	}

	cancelSet := make(map[int64]struct{})

	for {
		select {
		// Watch() requested
		case wreq := <-w.reqc:
			outc := make(chan WatchResponse, 1)
			// TODO: pass custom watch ID?
			ws := &watcherStream{
				initReq: *wreq,
				id:      -1,
				outc:    outc,
				// unbuffered so resumes won't cause repeat events
				recvc: make(chan *WatchResponse),
			}

			ws.donec = make(chan struct{})
			w.wg.Add(1)
			go w.serveSubstream(ws, w.resumec)

			// queue up for watcher creation/resume
			w.resuming = append(w.resuming, ws)
			if len(w.resuming) == 1 {
				// head of resume queue, can register a new watcher
				wc.Send(ws.initReq.toPB())
			}
		// new events from the watch client
		case pbresp := <-w.respc:
			switch {
			case pbresp.Created:
				// response to head of queue creation
				if ws := w.resuming[0]; ws != nil {
					w.addSubstream(pbresp, ws)
					w.dispatchEvent(pbresp)
					w.resuming[0] = nil
				}

				if ws := w.nextResume(); ws != nil {
					wc.Send(ws.initReq.toPB())
				}

			default:
				// dispatch to appropriate watch stream
				ok := w.dispatchEvent(pbresp)

				if ok {
					break
				}

				// watch response on unexpected watch id; cancel id
				if _, ok := cancelSet[pbresp.WatchId]; ok {
					break
				}

				cancelSet[pbresp.WatchId] = struct{}{}
				cr := &pb.WatchRequest_CancelRequest{
					CancelRequest: &pb.WatchCancelRequest{
						WatchId: pbresp.WatchId,
					},
				}
				req := &pb.WatchRequest{RequestUnion: cr}
				wc.Send(req)
			}

		// watch client failed on Recv; spawn another if possible
		case <-w.errc:
			if wc, closeErr = w.newWatchClient(); closeErr != nil {
				return
			}
			if ws := w.nextResume(); ws != nil {
				wc.Send(ws.initReq.toPB())
			}
			cancelSet = make(map[int64]struct{})

		case <-w.ctx.Done():
			return
		}
	}
}

// Watch posts a watch request to run() and waits for a new watcher channel
func (w *watcher) Watch(rtype pb.ResourceType, eType ...pb.EventType) WatchChan {
	wr := &watchRequest{
		// TODO: support cancellation
		ctx:      context.Background(),
		resource: rtype,
		event:    eType,
		retc:     make(chan chan WatchResponse, 1),
	}

	ok := false

	w.mu.Lock()
	if w.stream == nil {
		w.stream = w.newWatcherGrpcStream()
	}
	wgs := w.stream
	reqc := wgs.reqc
	w.mu.Unlock()

	// couldn't create channel; return closed channel
	closeCh := make(chan WatchResponse, 1)

	// submit request
	select {
	case reqc <- wr:
		ok = true
	case <-wr.ctx.Done():
	}

	// receive channel
	if ok {
		return <-wr.retc
	}

	close(closeCh)
	return closeCh
}
