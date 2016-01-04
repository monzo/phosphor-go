package phosphor

import (
	"net"
	"time"
)

type dispatcher interface {
	Dispatch([]chan []byte)
	Stop() error
}

type udpDispatcher struct {
	endpoint string

	conn net.Conn

	// channels used to receive traces
	oldChan chan []byte
	newChan chan []byte

	stopChan chan struct{}
}

func newUDPDispatcher(endpoint string) dispatcher {
	return &udpDispatcher{
		endpoint: endpoint,
		stopChan: make(chan struct{}),
	}, nil
}

// Dispatch causes the dispatcher to connect, and start consuming traces
// from the provided channels
func (u *udpDispatcher) Dispatch(oldChan, newChan chan []byte) error {
	u.oldChan = oldChan
	u.newChan = newChan

	if err := u.connect(); err != nil {
		return err
	}

	go u.dispatch()
}

// Stop our dispatcher, this can only be stopped once, and requires a new
// dispatcher to be created to resume sending
func (u *udpDispatcher) Stop() error {
	select {
	case <-u.stopChan:
	default:
		close(u.stopChan)
	}

	// wait for exit

	return nil
}

// connect our dispatcher
func (u *udpDispatcher) connect() error {
	c, err := net.DialTimeout("UDP", u.endpoint, 1*time.Second)
	if err != nil {
		return err
	}

	u.conn = c

	return nil
}

func (u *udpDispatcher) dispatch() {
	var b []byte
	for {
		select {
		case <-u.stopChan:
			return
		case b = <-u.oldChan:
		case b = <-u.newChan:
		}

		u.send(b)
	}
}

func (u *udpDispatcher) send(b []byte) {
	u.conn.Write(b)
}
