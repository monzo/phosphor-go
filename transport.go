package phosphor

import (
	"net"
	"time"
)

type transport interface {
	Consume(oldChan, newChan chan []byte)
	Stop() error
}

type udpTransport struct {
	endpoint string

	conn net.Conn

	// channels used to receive traces
	oldChan chan []byte
	newChan chan []byte

	stopChan chan struct{}
}

func newUDPTransport(endpoint string) transport {
	return &udpTransport{
		endpoint: endpoint,
		stopChan: make(chan struct{}),
	}, nil
}

// Consume causes the transport to connect, and start consuming traces
// from the provided channels
func (u *udpTransport) Consume(oldChan, newChan chan []byte) error {
	u.oldChan = oldChan
	u.newChan = newChan

	if err := u.connect(); err != nil {
		return err
	}

	go u.consume()
}

// Stop our transport, this can only be stopped once, and requires a new
// transport to be created to resume sending
func (u *udpTransport) Stop() error {
	select {
	case <-u.stopChan:
	default:
		close(u.stopChan)
	}

	// TODO wait for exit

	return nil
}

// connect our transport
func (u *udpTransport) connect() error {
	c, err := net.DialTimeout("UDP", u.endpoint, 1*time.Second)
	if err != nil {
		return err
	}

	u.conn = c

	return nil
}

// consume from our internal trace channels until we exit
func (u *udpTransport) consume() {
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

// send a []byte via our connection
func (u *udpTransport) send(b []byte) {
	u.conn.Write(b)
}
