package phosphor

import (
	"errors"
	"io"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"github.com/davegardnerisme/deephash"
	"github.com/golang/protobuf/proto"
	phos "github.com/mondough/phosphor/proto"
)

const (
	configForceReloadTime  = 1 * time.Minute
	defaultTraceBufferSize = 250 // 64KB max per trace, default max 16MB mem usage
)

var (
	// ErrTimeout represents a timeout error while attempting to send
	ErrTimeout = errors.New("timeout queueing annotation")
)

var (
	tracer *traceClient
)

type Phosphor struct {
	// ensure the client is initialized
	initOnce sync.Once

	// an io.Writer to which the traces are written
	// this is currently a UDP socket
	w io.Writer

	// traceChan internally buffers traces and passes these from
	// producers to the writers
	traceChan chan []byte

	// configMtx guards access to the config provider, and hash of the currently
	// loaded configuration
	configMtx sync.RWMutex

	// configProvider which returns configuration for our client
	configProvider ConfigProvider

	// configLastHash is the hash of the most recently used configuration
	// we use this to determine if we need to reinitialise on notification of a
	// config change
	configLastHash []byte

	// exitChan is closed when the client is shutting down
	// TODO refactor to use tomb
	exitChan chan struct{}
}

// New initialises and returns a Phosphor client
// This takes a config provider which can be a simple static config,
// or a more dynamic configuration loader which watches a remote config source
func New(configProvider ConfigProvider) (*Phosphor, error) {
	configProvider.Config().assertInitialized()

	// // TODO validate config
	// c := configProvider.Config()

	p := &Phosphor{
		configProvider: configProvider,
		// initialise traceChan with default length, we'll replace this
		// asynchronously with one of the correct length on first use
		traceChan: make(chan []byte, defaultTraceBufferSize),
		exitChan:  make(chan struct{}),
	}

	return p, nil
}

// Send an annotation to Phosphor
func (p *Phosphor) Send(a *phos.Annotation) error {
	// Initialise the tracer on first use
	p.initOnce.Do(p.init)

	// Marshal to bytes to be sent on the wire
	//
	// We're marshaling this here so that the marshalling can be executed
	// concurrently by any number of clients before pushing this to a single
	// worker goroutine for dispatch
	//
	// TODO future versions of this may use a more feature rich wire format
	b, err := proto.Marshal(a)
	if err != nil {
		return err
	}

	select {
	case p.traceChan <- b:
	case <-time.After(sendTimeout):
		log.Tracef("Timeout after %v attempting to queue trace annotation: %+v", sendTimeout, a)
		return ErrTimeout
	}

	return nil
}

// init should be called precisely once to initialise the client
func (p *Phosphor) init() {
	// fire config monitor which will immediately reinitialise the config
	// TODO control this with a tomb
	go p.monitorConfig()
}

// monitorConfig observes the config provider and triggers a reload of the
// configuration both when notified of a change and when a minimum time period
// has elapsed
func (p *Phosphor) monitorConfig() {
	configChange := p.configProvider.Notify()
	configTimer := time.NewTicker(configForceReloadTime)
	immediate := make(chan struct{}, 1)
	immediate <- struct{}{}

	for {
		select {
		case <-p.exitChan:
			configTimer.Stop()
			return
		case <-immediate:
		case <-configChange:
		case <-configTimer.C:
		}
		p.reloadConfig()
	}
}

// reloadConfig and reinitialise phosphor client if necessary
//
// Get Config
// Test hash of config to determine if changed
// If so, update config & reinit
func (p *Phosphor) reloadConfig() {
	c := p.configProvider.Config()
	h := deephash.Hash(c)
	if !p.configChanged(newHash) {
		return
	}

	// TODO actually re init things

	// keep reference to the old channel so we can drain this in parallel with
	// new traces the dispatcher receives
	oldChan := p.traceChan

	// init new channel for traces, ensure this *isn't* zero
	bufLen := c.BufferSize
	if bufLen == 0 {
		bufLen = defaultTraceBufferSize
	}
	newChan = make(chan []byte, bufLen)

	// init new tracer passing both channels to this
	// therefore it starts consuming from the new one (with nothing)
	// and also the old one (still current) in parallel to the previous tracer

	// gracefully shut down old tracer, so just the new one is running

	// swap the client reference of the trace channel from old to new, so
	// new clients start using the new resized channel

	// close old channel? in theory on the next config reload this will be
	// recycled, fall out of scope and garbage collected as it will have no
	// producers or consumers, so ideally not necessary, and avoids writing to
	// closed channel problems

}
