package ardb

import (
	"errors"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// StandardConnectionDialer defines a non-pooled standard connection dialer.
type StandardConnectionDialer struct{}

// Dial implements ConnetionDialer.Dial
func (scd StandardConnectionDialer) Dial(cfg ConnConfig) (Conn, error) {
	return Dial(cfg)
}

// NewPool creates a new pool for multiple ardb servers,
// if no dialFunc is given, a default one will be used instead,
// which established a tcp connection for the given connection info.
func NewPool(dial DialFunc) *Pool {
	if dial == nil {
		dial = Dial
	}

	return &Pool{
		pools:    make(map[ConnConfig]*redis.Pool),
		dialFunc: dial,
	}
}

// Pool maintains a collection of pools (one pool per config).
type Pool struct {
	mux      sync.RWMutex //protects following
	pools    map[ConnConfig]*redis.Pool
	dialFunc DialFunc
}

// Dial implements ConnectionDialer.Dial
func (p *Pool) Dial(cfg ConnConfig) (Conn, error) {
	conn := p.getConnectionSpecificPool(cfg).Get()
	return conn, conn.Err()
}

// GetConnectionSpecificPool gets a redis.Pool for a specific config.
func (p *Pool) getConnectionSpecificPool(cfg ConnConfig) *redis.Pool {
	p.mux.RLock()
	pool, ok := p.pools[cfg]
	p.mux.RUnlock()

	if ok {
		return pool
	}

	pool = &redis.Pool{
		MaxActive:   10,
		MaxIdle:     10,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return p.dialFunc(cfg) },
	}
	p.mux.Lock()
	p.pools[cfg] = pool
	p.mux.Unlock()

	return pool
}

// Close releases the resources used by the pool.
func (p *Pool) Close() {
	p.mux.Lock()
	defer p.mux.Unlock()

	// close all storage server pools
	for _, pool := range p.pools {
		pool.Close()
	}

	p.pools = nil
}

// ConnectionDialer defines a type which allows you to dial a connection.
type ConnectionDialer interface {
	// Dial an ARDB connection.
	// The callee must close the returned connection.
	Dial(cfg ConnConfig) (Conn, error)
}

// Dial a standard (TCP) connection using a given ARDB server config.
func Dial(cfg ConnConfig) (Conn, error) {
	return redis.Dial("tcp", cfg.Address, redis.DialDatabase(cfg.Database))
}

// DialFunc represents any kind of function,
// used to dial an ARDB connection.
type DialFunc func(cfg ConnConfig) (Conn, error)

// ConnConfig is the config that is used to dial a connection.
type ConnConfig struct {
	Address  string
	Database int
}

// Validate the given Connection Config,
// returning an error when no config is given,
// or in case an invalid one is given.
func (cfg ConnConfig) Validate() error {
	if cfg.Address == "" {
		return errAddressNotGiven
	}
	return nil
}

// Conn represents a connection to an ARDB server.
type Conn interface {
	// Close closes the connection.
	Close() error

	// Err returns a non-nil value when the connection is not usable.
	Err() error

	// Do sends a command to the server and returns the received reply.
	Do(commandName string, args ...interface{}) (reply interface{}, err error)

	// Send writes the command to the client's output buffer.
	Send(commandName string, args ...interface{}) error

	// Flush flushes the output buffer to the ARDB server.
	Flush() error

	// Receive receives a single reply from the ARDB server
	Receive() (reply interface{}, err error)
}

// Various connection-related errors returned by this package.
var (
	errAddressNotGiven = errors.New("ARDB server address not given")
)
