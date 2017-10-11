// Package redisstub is a minimal package
// providing redis-related (in-memory) implementations
// meant for testing and dev purposes only.
package redisstub

import (
	"errors"
	"io/ioutil"
	"os"

	"github.com/garyburd/redigo/redis"
	lediscfg "github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/server"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

// NewMemoryRedis creates a new in-memory redis stub.
// It must be noted that the stub only partially redis-compliant,
// not all commands (such as MULTI/EXEC) are supported.
// All available commands can be found at:
// https://github.com/siddontang/ledisdb/blob/master/doc/commands.md
// WARNING: should be used for testing/dev purposes only!
func NewMemoryRedis() *MemoryRedis {
	cfg := lediscfg.NewConfigDefault()
	cfg.DBName = "memory"
	cfg.DataDir, _ = ioutil.TempDir("", "redisstub")
	// assigning the empty string to Addr,
	// such that it auto-assigns a free local port
	cfg.Addr = ""

	app, err := server.NewApp(cfg)
	if err != nil {
		log.Fatalf("couldn't create embedded ledisdb: %s", err.Error())
	}

	mr := &MemoryRedis{
		app:     app,
		addr:    app.Address(),
		datadir: cfg.DataDir,
	}
	go mr.listen()
	return mr
}

// MemoryRedis is an in memory redis connection implementation
type MemoryRedis struct {
	app     *server.App
	addr    string
	datadir string
}

// Dial to the embedded Go Redis Server,
// and return the established connection if possible.
func (mr *MemoryRedis) Dial(connectionString string, database int) (redis.Conn, error) {
	if mr == nil {
		return nil, errors.New("no in-memory redis is available")
	}

	return redis.Dial("tcp", mr.addr, redis.DialDatabase(database))
}

// Close the embedded Go Redis Server,
// and delete the used datadir.
func (mr *MemoryRedis) Close() {
	if mr == nil {
		return
	}

	os.Remove(mr.datadir)
	mr.app.Close()
}

// address returns the tcp (local) address of this MemoryRedis server
func (mr *MemoryRedis) address() string {
	if mr == nil {
		return ""
	}

	return mr.addr
}

// StorageServerConfig returns a new StorageServerConfig,
// usable to connect to this in-memory redis-compatible ledisdb.
func (mr *MemoryRedis) StorageServerConfig() config.StorageServerConfig {
	return config.StorageServerConfig{Address: mr.address()}
}

// Listen to any incoming TCP requests,
// and process them in the embedded Go Redis Server.
func (mr *MemoryRedis) listen() {
	log.Info("embedded LedisDB Server ready and listening at ", mr.addr)
	mr.app.Run()
}

// NewMemoryRedisSlice creates a slice of in-memory redis stubs.
func NewMemoryRedisSlice(n int) *MemoryRedisSlice {
	if n <= 0 {
		panic("invalid memory redis slice count")
	}

	var slice []*MemoryRedis
	for i := 0; i < n; i++ {
		slice = append(slice, NewMemoryRedis())
	}
	return &MemoryRedisSlice{slice: slice}
}

// MemoryRedisSlice is a slice of in memory redis connection implementations
type MemoryRedisSlice struct {
	slice []*MemoryRedis
}

// StorageClusterConfig returns a new StorageClusterConfig,
// usable to connect to this slice of in-memory redis-compatible ledisdb.
func (mrs *MemoryRedisSlice) StorageClusterConfig() config.StorageClusterConfig {
	var cfg config.StorageClusterConfig
	if mrs == nil || mrs.slice == nil {
		return cfg
	}

	for _, server := range mrs.slice {
		cfg.Servers = append(cfg.Servers,
			config.StorageServerConfig{Address: server.address()})
	}
	return cfg
}

// Close implements ConnProvider.Close
func (mrs *MemoryRedisSlice) Close() error {
	for _, memRedis := range mrs.slice {
		memRedis.Close()
	}
	return nil
}
