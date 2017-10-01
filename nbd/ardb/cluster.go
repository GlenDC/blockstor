package ardb

import (
	"errors"
	"sync"

	"github.com/zero-os/0-Disk/config"
)

// HotReloading of a cluster shouldn't be part of this package,
// as it isn't generic enough, and it wouldn't be as efficient as it could be!

// TODO:
// improve the API of the Cluster struct type,
// such that it can be easily embedded in another type such as ClusterPair,
// where we have not just primary servers but also secondary (backup) servers.

// StorageCluster TODO: finish
type StorageCluster interface {
	ServerConfig() (*ServerConfig, error)
	ServerConfigFor(objectIndex int64) (*ServerConfig, error)
	ServerIndexFor(objectIndex int64) (int64, error)
}

// NewCluster creates a new ARDB Cluster.
func NewCluster(clusterType ClusterType, servers ...config.StorageServerConfig) (*Cluster, error) {
	cluster := new(Cluster)
	err := cluster.SetServers(clusterType, servers...)
	if err != nil {
		return nil, err
	}
	return cluster, nil
}

// Cluster represents the in-memory state
// as well as the API of a static storage cluster.
// This type cannot be used on multiple goroutines.
type Cluster struct {
	servers     []config.StorageServerConfig
	serverCount int64
	clusterType ClusterType
	mux         sync.RWMutex
}

// SetServers allows you to overwrite the clusterType AND servers of this cluster,
// basically setting all the data this cluster uses.
// It's a function that can be used to implement the hot reloading of data.
func (cluster *Cluster) SetServers(clusterType ClusterType, servers ...config.StorageServerConfig) error {
	if len(servers) == 0 {
		return ErrInsufficientServers
	}
	err := clusterType.Validate()
	if err != nil {
		return err
	}

	cluster.mux.Lock()
	cluster.servers = servers
	cluster.serverCount = int64(len(servers))
	cluster.clusterType = clusterType
	cluster.mux.Unlock()
	return nil
}

// ServerStateAt returns the state of a server at a given index.
func (cluster *Cluster) ServerStateAt(serverIndex int64) (config.StorageServerState, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	if serverIndex < 0 || serverIndex >= cluster.serverCount {
		return config.StorageServerStateUnknown, ErrServerIndexOOB
	}
	return cluster.servers[serverIndex].State, nil
}

// SetServerStateAt sets the state for a server at a given index,
// returning true if the state in question wasn't set yet.
func (cluster *Cluster) SetServerStateAt(serverIndex int64, state config.StorageServerState) (bool, error) {
	currentState, err := cluster.ServerStateAt(serverIndex)
	if err != nil || currentState == state {
		return false, err // err or nothing to do
	}

	cluster.mux.Lock()
	cluster.servers[serverIndex].State = state
	cluster.mux.Unlock()
	return true, nil
}

// ServerConfig returns the first available server.
func (cluster *Cluster) ServerConfig() (*ServerConfig, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	for serverIndex, server := range cluster.servers {
		if server.State == config.StorageServerStateOnline {
			return &ServerConfig{
				Address:  server.Address,
				Database: server.Database,
				Index:    int64(serverIndex),
				Type:     cluster.clusterType,
			}, nil
		}
	}

	return nil, ErrNoServersAvailable
}

// ServerConfigFor returns the ServerConfig for a given object index.
func (cluster *Cluster) ServerConfigFor(objectIndex int64) (*ServerConfig, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	// get the server index of an operational server of this cluster
	serverIndex, err := cluster.serverIndex(objectIndex)
	if err != nil {
		return nil, err
	}
	server := cluster.servers[objectIndex]

	// return the configuration for the operational server
	return &ServerConfig{
		Address:  server.Address,
		Database: server.Database,
		Index:    serverIndex,
		Type:     cluster.clusterType,
	}, nil
}

// ServerIndexFor returns the server index of a server,
// for a given index of an object.
func (cluster *Cluster) ServerIndexFor(objectIndex int64) (int64, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()
	return cluster.serverIndex(objectIndex)
}

func (cluster *Cluster) serverIndex(objectIndex int64) (int64, error) {
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
	Type     ClusterType
}

// ClusterType defines a type of ardb server.
type ClusterType uint8

// String returns the ClusterType as a string.
func (ct ClusterType) String() string {
	switch ct {
	case ClusterTypePrimary:
		return "primary"
	case ClusterTypeSlave:
		return "slave"
	case ClusterTypeTemplate:
		return "template"
	default:
		return ""
	}
}

// Validate this ClusterType,
// returning an error in case the cluster type isn't valid.
func (ct ClusterType) Validate() error {
	if ct >= ClusterTypeCount {
		return ErrInvalidClusterType
	}
	return nil
}

// The different server types available.
const (
	ClusterTypePrimary ClusterType = iota
	ClusterTypeSlave
	ClusterTypeTemplate
	ClusterTypeCount
)

var (
	// ErrInvalidClusterType is returned in acse
	// a cluster type is invalid.
	ErrInvalidClusterType = errors.New("invalid cluster type")
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
