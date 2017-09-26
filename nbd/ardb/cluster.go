package ardb

import (
	"errors"
	"sync"

	"github.com/zero-os/0-Disk/config"
)

// TODO:
// how to set server states and react on those changes?

// TODO:
// should we simply make the dynamic reloading a standalone spawned goroutine
// which manages a cluster?!

// NewCluster creates a new ARDB Cluster.
func NewCluster(servers ...config.StorageServerConfig) *Cluster {
	return &Cluster{
		servers:     servers,
		serverCount: int64(len(servers)),
	}
}

// Cluster represents the in-memory state
// as well as the API of a static storage cluster.
// This type cannot be used on multiple goroutines.
type Cluster struct {
	servers     []config.StorageServerConfig
	serverCount int64
	serverType  ServerType

	slaveServers     []config.StorageServerConfig
	slaveServerCount int64
	slaveServerType  ServerType

	mux sync.RWMutex
}

// SetPrimaryServers allows you to set primary servers for the given cluster.
// You need to give at least as much servers as the slave servers defined,
// an error will be returned oterhwise.
func (cluster *Cluster) SetPrimaryServers(servers ...config.StorageServerConfig) error {
	count := int64(len(servers))

	cluster.mux.Lock()
	defer cluster.mux.Unlock()

	if count == 0 || count < cluster.slaveServerCount {
		return ErrInsufficientServers
	}
	cluster.servers = servers
	cluster.serverCount = count

	return nil
}

// SetSlaveServers allows you to (un)set slave servers for the given cluster.
// You need to give at least as much servers as the primary servers defined,
// an error will be returned oterhwise
func (cluster *Cluster) SetSlaveServers(servers ...config.StorageServerConfig) error {
	cluster.mux.Lock()
	defer cluster.mux.Unlock()

	count := int64(len(servers))
	if servers != nil && count < cluster.serverCount {
		return ErrInsufficientServers
	}

	cluster.slaveServers = servers
	cluster.slaveServerCount = count
	return nil
}

// ServerConfig returns the ServerConfig for a given object index.
func (cluster *Cluster) ServerConfig(index int64) (*ServerConfig, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	// get the index of an operational primary or slave server
	serverIndex, err := cluster.serverIndex(index)
	if err != nil {
		return nil, err
	}

	// if we get a valid index,
	// we know for sure that it is either for a primary or slave server,
	// which is operational.
	//
	// TODO: this serverType could be any of the following combinations:
	//   1. primary | slave
	//   2. slave   | primary
	//   3. primary | N/A
	// ... now however it is assumed to be always situation (1)
	server, serverType := cluster.servers[index], ServerTypePrimary
	if server.State != config.StorageServerStateOnline {
		server, serverType = cluster.slaveServers[index], ServerTypeSlave
	}

	// return the configuration for the operational server
	return &ServerConfig{
		Address:  server.Address,
		Database: server.Database,
		Index:    serverIndex,
		Type:     serverType,
	}, nil
}

// ServerIndex returns the (modulo) index of a server,
// based on a given index of an object.
func (cluster *Cluster) ServerIndex(index int64) (int64, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()
	return cluster.serverIndex(index)
}

// serverIndex returns the (modulo) index of a server,
// based on a given index of an object.
func (cluster *Cluster) serverIndex(index int64) (int64, error) {
	// first try the modulo sharding,
	// which will work for all default online shards
	// and thus keep it as cheap as possible
	serverIndex := index % cluster.serverCount
	if cluster.servers[serverIndex].State == config.StorageServerStateOnline {
		return serverIndex, nil
	}

	// ensure that we have servers which are actually online
	if !cluster.operational() {
		return -1, ErrNoServersAvailable
	}

	// keep trying until we find an online server
	// in the same reproducable manner
	// (another kind of tracing)
	// using jumpConsistentHash taken from https://arxiv.org/pdf/1406.2294.pdf
	var j int64
	var key uint64
	for {
		key = uint64(index)
		j = 0
		for j < cluster.serverCount {
			serverIndex = j
			key = key*2862933555777941757 + 1
			j = int64(float64(serverIndex+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
		}
		if cluster.servers[serverIndex].State == config.StorageServerStateOnline {
			return serverIndex, nil
		}

		index++
	}
}

// operational returns true if
// at least one server of the given cluster is online.
func (cluster *Cluster) operational() bool {
	for index := range cluster.servers {
		if cluster.serverOperational(int64(index)) {
			return true
		}
	}
	return false
}

// serverOperational returns true if
// the primary or slave server for the given index is online.
func (cluster *Cluster) serverOperational(index int64) bool {
	if cluster.servers[index].State == config.StorageServerStateOnline {
		return true
	}

	if cluster.slaveServers != nil && cluster.slaveServers[index].State == config.StorageServerStateOnline {
		return true
	}

	return false
}

// ServerConfig defines the config for an ARDB server.
type ServerConfig struct {
	Address  string
	Database int
	Index    int64
	Type     ServerType
}

// ClusterType defines a type of ardb cluster.
type ClusterType uint8

// The different cluster types available.
const (
	ClusterTypePrimarySlave ClusterType = iota
	ClusterTypeSlavePrimary
	ClusterTypeTemplate
)

// String returns the ServerType as a string.
func (ct ClusterType) String() string {
	switch ct {
	case ClusterTypePrimarySlave:
		return "primary-to-slave"
	case ClusterTypeSlavePrimary:
		return "slave-to-primary"
	case ClusterTypeTemplate:
		return "template"
	default:
		return ""
	}
}

// ServerType defines a type of ardb server.
type ServerType uint8

// String returns the ServerType as a string.
func (st ServerType) String() string {
	switch st {
	case ServerTypePrimary:
		return "primary"
	case ServerTypeSlave:
		return "slave"
	case ServerTypeTemplate:
		return "template"
	default:
		return ""
	}
}

// The different server types available.
const (
	ServerTypePrimary ServerType = iota
	ServerTypeSlave
	ServerTypeTemplate
)

var (
	// ErrNoServersAvailable is returned in case
	// no servers are available for usage.
	ErrNoServersAvailable = errors.New("no servers available")
	// ErrInsufficientServers is returned in case for a given situation
	// the amount of specified servers is not sufficient for its use case.
	ErrInsufficientServers = errors.New("insufficient servers configured")
)
