package phosphor

import (
	"errors"
	"fmt"
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

// Phosphor is a client which sends annotations to the phosphor server. This
// should be initialised with New() rather than directly
type Phosphor struct {
	// ensure the client is initialized
	initOnce sync.Once

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

	// the transport is used to send traces onto phosphor or phosphorD
	tr    transport
	trMtx sync.Mutex
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
	// worker goroutine for transport
	//
	// TODO future versions of this may use a more feature rich wire format
	b, err := proto.Marshal(a)
	if err != nil {
		return err
	}

	c := p.configProvider.Config()

	select {
	case p.traceChan <- b:
	case <-time.After(c.SendTimeout):
		log.Tracef("Timeout after %v attempting to queue trace annotation: %+v", c.SendTimeout, a)
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
		if err := p.reloadConfig(); err != nil {
			log.Warnf("[Phosphor] Failed to reload configuration: %v", err)
		}
	}
}

func (p *Phosphor) compareConfigHash(h []byte) bool {
	p.configMtx.RLock()
	defer p.configMtx.RUnlock()
	return p.configLastHash == h
}

func (p *Phosphor) updateConfigHash(h []byte) {
	p.configMtx.Lock()
	p.configLastHash = h
	p.configMtx.Unlock()
	return
}

// reloadConfig and reinitialise phosphor client if necessary
//
// Get Config
// Test hash of config to determine if changed
// If so, update config & reinit
func (p *Phosphor) reloadConfig() error {
	c := p.configProvider.Config()
	h := deephash.Hash(c)

	// Skip reloading if the config is the same
	if p.compareConfigHash(newHash) {
		return nil
	}

	// keep reference to the old channel so we can drain this in parallel with
	// new traces the transport receives
	oldChan := p.traceChan

	// init new channel for traces, ensure this *isn't* zero
	bufLen := c.BufferSize
	if bufLen == 0 {
		bufLen = defaultTraceBufferSize
	}
	newChan := make(chan []byte, bufLen)

	// Get a new transport and keep a reference to the old one
	p.trMtx.Lock()
	defer p.trMtx.Unlock()
	oldTr := p.tr
	endpoint := fmt.Sprintf("%s:%v", c.Host, c.Port)
	newTr := newUDPTransport(endpoint)

	// start new transport by passing both channels to this
	// therefore it starts consuming from the new one (with nothing)
	// and also the old one (still current) in parallel to the previous transport
	// If this somehow fails, abort until next attempt
	if err := newTr.Consume(oldChan, newChan); err != nil {
		newTr.Stop()
		return err
	}

	// swap the client reference of the trace channel from old to new, so
	// new clients start using the new resized channel
	// TODO atomically swap this
	p.traceChan = newChan

	// gracefully shut down old transport, so just the new one is running
	if err := oldTr.Stop(); err != nil {
		return err
	}

	// set the config hash & swap the transport as we're finished
	p.updateConfigHash(h)
	p.tr = newTr

	return nil
}
