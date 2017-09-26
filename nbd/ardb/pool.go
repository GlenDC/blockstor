package ardb

import (
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// ConnectionGetter defines a type which allows you to get a connection.
type ConnectionGetter interface {
	// GetConnection gets a connection. The application must close the returned connection.
	// This method always returns a valid connection so that applications can defer
	// error handling to the first use of the connection. If there is an error
	// getting an underlying connection, then the connection Err, Do, Send, Flush
	// and Receive methods return that error.
	GetConnection(address string, database int) redis.Conn
}

// NewPool creates a new pool for multiple ardb servers,
// if no dialFunc is given, a default one will be used instead,
// which established a tcp connection for the given connection info.
func NewPool(dial DialFunc) *Pool {
	if dial == nil {
		dial = defaultDialFunc
	}

	return &Pool{
		pools: make(map[connConfig]*redis.Pool),
		Dial:  dial,
	}
}

// Pool maintains a collection of redis.Pool's per connection's database.
// The application calls the Get method
// to get a database connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The normal redigo.Pool is not adequate since it only maintains connections for a single server.
type Pool struct {
	mux   sync.RWMutex //protects following
	pools map[connConfig]*redis.Pool

	Dial DialFunc
}

// GetConnection implements ConnectionGetter.GetConnection
func (p *Pool) GetConnection(address string, database int) redis.Conn {
	return p.getConnectionSpecificPool(connConfig{address, database}).Get()
}

// GetConnectionSpecificPool gets a redis.Pool for a specific connectionString.
func (p *Pool) getConnectionSpecificPool(cfg connConfig) *redis.Pool {
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
		Dial:        func() (redis.Conn, error) { return p.Dial(cfg.address, cfg.database) },
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

// connConfig is used to map a connection's config to its pool
type connConfig struct {
	address  string
	database int
}

// DialFunc creates a redis.Conn (if possible),
// based on a given connectionString and database.
type DialFunc func(address string, database int) (redis.Conn, error)

// StandardConnectionGetter defines a non-pooled standard connection getter.
type StandardConnectionGetter struct{}

// GetConnection implements ConnectionGetter.GetConnection
func (scg StandardConnectionGetter) GetConnection(address string, database int) redis.Conn {
	conn, err := redis.Dial("tcp", address, redis.DialDatabase(database))
	if err != nil {
		return errorConnection{err}
	}

	return conn
}

// defaultDialFunc is used when creating a Pool,
// without a custom-defined DialFunc (and given nil instead).
func defaultDialFunc(address string, database int) (redis.Conn, error) {
	return redis.Dial("tcp", address, redis.DialDatabase(database))
}
