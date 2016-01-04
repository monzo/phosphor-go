package phosphor

import (
	"net"
	"time"

	"gopkg.in/tomb.v2"
)

// transport consumes traces and sends these onto a phosphor server
type transport interface {
	Consume(oldChan, newChan chan []byte) error
	Stop() error
}

// udpTransport is a concrete implementation of the transport interface which
// sends trace data via UDP
type udpTransport struct {
	endpoint string

	// conn is the UDP connection used to send
	conn net.Conn

	// channels used to receive traces
	oldChan chan []byte
	newChan chan []byte

	// tomb is used to control our worker(s) consuming and sending
	t tomb.Tomb
}

// newUDPTransport returns an unconnected UDP transport
func newUDPTransport(endpoint string) transport {
	return &udpTransport{
		endpoint: endpoint,
	}
}

// Consume causes the transport to connect, and start consuming traces
// from the provided channels
func (u *udpTransport) Consume(oldChan, newChan chan []byte) error {
	u.oldChan = oldChan
	u.newChan = newChan

	if err := u.connect(); err != nil {
		return err
	}

	u.t.Go(u.consume)
	return nil
}

// Stop our transport, this can only be stopped once, and requires a new
// transport to be created to resume sending
func (u *udpTransport) Stop() error {
	u.t.Kill(nil)
	return u.t.Wait()
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
func (u *udpTransport) consume() error {
	var b []byte
	for {
		select {
		case <-u.t.Dying():
			return nil
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
