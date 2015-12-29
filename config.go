package phosphor

import "time"

// Config used by our client
type Config struct {
	initialized bool

	Host string
	Port int

	// BufferSize is the number of events we hold in memory in the client
	// when these are being published asynchronously
	BufferSize int32

	// Sending settings
	SendTimeout   time.Duration
	MaxPacketSize int32
}

// NewConfig returns a Config with default settings initialized
func NewConfig() {
	return &Config{
		initialized:   true,
		Host:          "localhost",
		Port:          7760,
		SendTimeout:   5 * time.Millisecond,
		BufferSize:    250,            // 64KB each, max 16MB mem usage
		MaxPacketSize: 65536 - 8 - 20, // 8-byte UDP header, 20-byte IP header
	}
}

func (c *Config) assertInitialized() {
	if c == nil || !c.initialized {
		panic("Config{} must be created with NewConfig()")
	}
}

// Notify returns a channel which fires on configuration change.
//
// In the case of a standard (static) Config type this returns a channel which
// will *never* fire, and *always* blocks, however this does satisfy the
// ConfigProvider interface and makes sense as the config never changes.
//
// Other implementations which dynamically reload configuration
// can use this to trigger a configuration reload, and reinitialisation of
// the client while the program executes, during which traces will buffer up to
// the configured in memory buffer size
func (c *Config) Notify() <-chan struct{} {
	ch := make(chan struct{})
	return ch
}

// Config returns the config, so that Config satisfies the ConfigProvider
// interface. Note this returns a *clone* of the configuration to isolate
// readers from changes
func (c *Config) Config() *Config {
	// Guard against nil pointer dereference
	if c == nil {
		return &Config{}
	}

	// Return a clone of the config so that this is immutable for the receiver
	c2 := *c
	return &c2
}

type ConfigProvider interface {
	// Notify returns a channel which fires on config changes
	Notify() <-chan struct{}

	// Config returns the raw configuration
	Config() *Config
}
