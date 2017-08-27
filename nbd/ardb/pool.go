package ardb

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// NewRedisPoolFactory creates a new redis pool factory,
// using the given dial func
func NewRedisPoolFactory(dial DialFunc) RedisPoolFactory {
	return func() *RedisPool {
		return NewRedisPool(dial)
	}
}

// RedisPoolFactory defines a factory,
// to be used to create a new redis pool
type RedisPoolFactory func() *RedisPool

// DialFunc creates a redis.Conn (if possible),
// based on a given connectionString and database.
type DialFunc func(connectionString string, database int) (redis.Conn, error)

// NewRedisPool creates a new pool for multiple redis servers,
// if no dialFunc is given, a default one will be used instead,
// which established a tcp connection for the given connection info.
func NewRedisPool(dial DialFunc) (p *RedisPool) {
	if dial == nil {
		dial = defaultRedisDialFunc
	}

	return &RedisPool{
		connectionSpecificPools: new(connectionPool),
		Dial: dial,
	}
}

// RedisPool maintains a collection of redis.Pool's per connection's database.
// The application calls the Get method
// to get a database connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The normal redigo.Pool is not adequate since it only maintains connections for a single server.
type RedisPool struct {
	connectionSpecificPools *connectionPool

	Dial DialFunc
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *RedisPool) Get(connectionString string, database int) redis.Conn {
	return p.getConnectionSpecificPool(connectionString, database).Get()
}

// GetConnectionSpecificPool gets a redis.Pool for a specific connectionString.
func (p *RedisPool) getConnectionSpecificPool(connectionString string, database int) (singleServerPool *redis.Pool) {
	rawPool, ok := p.connectionSpecificPools.Load(connectionString)
	if !ok {
		pool := &databasePool{}
		p.connectionSpecificPools.Store(connectionString, pool)
		rawPool = pool
	}
	pool := rawPool.(*databasePool)

	rawRPool, ok := pool.Load(database)
	if !ok {
		rpool := &redis.Pool{
			MaxActive:   10,
			MaxIdle:     10,
			Wait:        true,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				return p.Dial(connectionString, database)
			},
		}
		pool.Store(database, rpool)
		rawRPool = rpool
	}

	return rawRPool.(*redis.Pool)
}

// Close releases the resources used by the pool.
func (p *RedisPool) Close() {
	p.connectionSpecificPools.Range(func(_, v interface{}) bool {
		(v.(*databasePool)).Range(func(_, v interface{}) bool {
			(v.(*redis.Pool)).Close()
			return true
		})
		return true
	})

	// create a new connection specific pool
	p.connectionSpecificPools = new(connectionPool)
}

// defaultRedisDialFunc is used when creating a RedisPool,
// without a custom-defined DialFunc (and given nil instead).
func defaultRedisDialFunc(connectionString string, database int) (redis.Conn, error) {
	return redis.Dial("tcp", connectionString, redis.DialDatabase(database))
}
