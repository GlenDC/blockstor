package ardb

import (
	"errors"

	"github.com/zero-os/0-Disk/config"
)

// HotReloading of a cluster shouldn't be part of this package,
// as it isn't generic enough, and it wouldn't be as efficient as it could be!

// StorageCluster TODO: finish
type StorageCluster interface {
	ServerConfig() (*ServerConfig, error)
	ServerConfigAt(serverIndex int64) (*ServerConfig, error)
	ServerConfigFor(objectIndex int64) (*ServerConfig, error)
	ServerIndexFor(objectIndex int64) (int64, error)
}

// TODO:
// how to set server states and react on those changes?

// TODO:
// should we simply make the dynamic reloading a standalone spawned goroutine
// which manages a cluster?!

// ClusterPair TODO
type ClusterPair struct {
	first, second *Cluster
}

// TODO: define ClusterPair methods

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

	// TODO:
	// + add cluster type
	// + make goroutine safe
}

// SetServers TODO
func (cluster *Cluster) SetServers(servers ...config.StorageServerConfig) error {
	return errors.New("TODO")
}

// SetServerState TODO
func (cluster *Cluster) SetServerState(serverIndex int64, state config.StorageServerState) error {
	return errors.New("TODO")
}

// ServerConfig returns the first available server.
func (cluster *Cluster) ServerConfig() (*ServerConfig, error) {
	for serverIndex, server := range cluster.servers {
		if server.State == config.StorageServerStateOnline {
			return &ServerConfig{
				Address:  server.Address,
				Database: server.Database,
				Index:    int64(serverIndex),
			}, nil
		}
	}

	return nil, ErrNoServersAvailable
}

// ServerConfigAt returns the ServerConfig at a given server index.
func (cluster *Cluster) ServerConfigAt(serverIndex int64) (*ServerConfig, error) {
	if serverIndex < 0 || serverIndex >= cluster.serverCount {
		return nil, ErrServerIndexOOB
	}

	server := cluster.servers[serverIndex]
	if server.State != config.StorageServerStateOnline {
		return nil, ErrServerNotOnline
	}

	// return the configuration for the operational server
	return &ServerConfig{
		Address:  server.Address,
		Database: server.Database,
		Index:    serverIndex,
	}, nil
}

// ServerConfigFor returns the ServerConfig for a given object index.
func (cluster *Cluster) ServerConfigFor(objectIndex int64) (*ServerConfig, error) {
	// get the server index of an operational server of this cluster
	serverIndex, err := cluster.ServerIndexFor(objectIndex)
	if err != nil {
		return nil, err
	}
	server := cluster.servers[objectIndex]

	// return the configuration for the operational server
	return &ServerConfig{
		Address:  server.Address,
		Database: server.Database,
		Index:    serverIndex,
		//Type:     serverType, // TODO
	}, nil
}

// ServerIndexFor returns the server index of a server,
// for a given index of an object.
func (cluster *Cluster) ServerIndexFor(objectIndex int64) (int64, error) {
	// first try the modulo sharding,
	// which will work for all default online shards
	// and thus keep it as cheap as possible
	serverIndex := objectIndex % cluster.serverCount
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
		key = uint64(objectIndex)
		j = 0
		for j < cluster.serverCount {
			serverIndex = j
			key = key*2862933555777941757 + 1
			j = int64(float64(serverIndex+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
		}
		if cluster.servers[serverIndex].State == config.StorageServerStateOnline {
			return serverIndex, nil
		}

		objectIndex++
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
	return cluster.servers[index].State == config.StorageServerStateOnline
}

// ServerConfig defines the config for an ARDB server.
type ServerConfig struct {
	Address  string
	Database int
	Index    int64
	Type     ServerType
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
	// ErrServerNotOnline is returned in case
	// a server is requested which isn't online according to its state.
	ErrServerNotOnline = errors.New("server is not online")
	// ErrServerIndexOOB is returned in case
	// a given server index used is out of bounds,
	// for the cluster context it is used within.
	ErrServerIndexOOB = errors.New("server index out of bounds")
	// ErrNoServersAvailable is returned in case
	// no servers are available for usage.
	ErrNoServersAvailable = errors.New("no servers available")
	// ErrInsufficientServers is returned in case for a given situation
	// the amount of specified servers is not sufficient for its use case.
	ErrInsufficientServers = errors.New("insufficient servers configured")
)
