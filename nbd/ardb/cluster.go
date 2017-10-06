package ardb

import (
	"errors"
	"sync"

	"github.com/zero-os/0-Disk/config"
)

// ConnProvider blaa...
type ConnProvider interface {
	// Connection dials a connection
	// to the first available server of this cluster.
	Connection() (Conn, error)
	// Connection dials a connection to a server within this cluster,
	// and which maps to the given objectIndex (taking the cluster state into account).
	ConnectionFor(objectIndex int64) (Conn, error)
}

// NewCluster creates a new static (ARDB) cluster.
func NewCluster(cfg config.StorageClusterConfig, dialer ConnectionDialer) (*Cluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if dialer == nil {
		dialer = new(StandardConnectionDialer)
	}
	return &Cluster{
		servers:     cfg.Servers,
		serverCount: int64(len(cfg.Servers)),
		dialer:      dialer,
	}, nil
}

// Cluster defines the in memory cluster
type Cluster struct {
	servers     []config.StorageServerConfig
	serverCount int64
	dialer      ConnectionDialer
	mux         sync.RWMutex
}

// Connection implements ConnProvider.Connection
func (cluster *Cluster) Connection() (Conn, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	for _, server := range cluster.servers {
		if server.State == config.StorageServerStateOnline {
			return cluster.dialer.Dial(ConnConfig{
				Address:  server.Address,
				Database: server.Database,
			})
		}
	}

	return nil, ErrNoServersAvailable
}

// ConnectionFor implements ConnProvider.ConnectionFor
func (cluster *Cluster) ConnectionFor(objectIndex int64) (Conn, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	serverIndex, err := ComputeServerIndex(cluster.serverCount, objectIndex, cluster.serverIsOnline)
	if err != nil {
		return nil, err
	}

	server := cluster.servers[serverIndex]
	return cluster.dialer.Dial(ConnConfig{
		Address:  server.Address,
		Database: server.Database,
	})
}

// SetServers allows you to set the servers used by this cluster.
func (cluster *Cluster) SetServers(servers []config.StorageServerConfig) error {
	if len(servers) == 0 {
		return ErrInvalidInput
	}

	cluster.mux.Lock()
	cluster.servers = servers
	cluster.serverCount = int64(len(servers))
	cluster.mux.Unlock()

	return nil
}

// SetServerState sets the state of a server.
func (cluster *Cluster) SetServerState(serverIndex int64, state config.StorageServerState) error {
	cluster.mux.Lock()
	if serverIndex < 0 || serverIndex > cluster.serverCount {
		cluster.mux.Unlock()
		return ErrServerIndexOOB
	}

	cluster.servers[serverIndex].State = state
	cluster.mux.Unlock()
	return nil
}

// serverOperational returns true if
// the server on the given index is online.
func (cluster *Cluster) serverIsOnline(index int64) bool {
	return cluster.servers[index].State == config.StorageServerStateOnline
}

// TODO: define this fucking clusterPair
// the idea is there, but the implementation is shit
// need to make sure we can lock properly
// .... might be better to define an entirely new type though
// .... as this looks like shiiit

type ClusterPair struct {
	First  *Cluster
	Second *Cluster
}

// Connection implements ConnProvider.Connection
func (pair *ClusterPair) Connection() (Conn, error) {
	var server config.StorageServerConfig
	for index := int64(0); index < pair.First.serverCount; index++ {
		// check if primary server is online
		server = pair.First.servers[index]
		if server.State == config.StorageServerStateOnline {
			return pair.First.dialer.Dial(ConnConfig{
				Address:  server.Address,
				Database: server.Database,
			})
		}
		if server.State != config.StorageServerStateOffline || pair.Second == nil {
			continue
		}

		// if slave servers are available, and the primary server state is offline,
		// let's try to get that slave server
		server = pair.Second.servers[index]
		if server.State == config.StorageServerStateOnline {
			return pair.Second.dialer.Dial(ConnConfig{
				Address:  server.Address,
				Database: server.Database,
			})
		}
	}

	return nil, ErrNoServersAvailable
}

// ConnectionFor implements ConnProvider.ConnectionFor
func (pair *ClusterPair) ConnectionFor(objectIndex int64) (Conn, error) {
	serverIndex, err := ComputeServerIndex(pair.First.serverCount, objectIndex, pair.serverIsOnline)
	if err != nil {
		return nil, err
	}

	server := pair.First.servers[serverIndex]
	if server.State == config.StorageServerStateOnline {
		return pair.First.dialer.Dial(ConnConfig{
			Address:  server.Address,
			Database: server.Database,
		})
	}

	server = pair.Second.servers[serverIndex]
	return pair.Second.dialer.Dial(ConnConfig{
		Address:  server.Address,
		Database: server.Database,
	})
}

// serverOperational returns true if
// a server on the given index is online.
func (pair *ClusterPair) serverIsOnline(index int64) bool {
	server := pair.First.servers[index]
	if server.State == config.StorageServerStateOnline {
		return true
	}
	if server.State != config.StorageServerStateOffline || pair.Second == nil {
		return false
	}

	server = pair.Second.servers[index]
	return server.State == config.StorageServerStateOnline
}

// ComputeServerIndex computes a server index for a given objectIndex,
// using a shared static algorithm with the serverCount as input and
// a given predicate to define if a computed index is fine.
func ComputeServerIndex(serverCount, objectIndex int64, predicate func(serverIndex int64) bool) (int64, error) {
	// first try the modulo sharding,
	// which will work for all default online shards
	// and thus keep it as cheap as possible
	serverIndex := objectIndex % serverCount
	if predicate(serverIndex) {
		return serverIndex, nil
	}

	// ensure that we have servers which are actually online
	var operational bool
	for i := int64(0); i < serverCount; i++ {
		if predicate(i) {
			operational = true
			break
		}
	}
	if !operational {
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
		for j < serverCount {
			serverIndex = j
			key = key*2862933555777941757 + 1
			j = int64(float64(serverIndex+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
		}
		if predicate(serverIndex) {
			return serverIndex, nil
		}

		objectIndex++
	}
}

var (
	// ErrNoServersAvailable is returned in case
	// no servers are available for usage.
	ErrNoServersAvailable = errors.New("no servers available")
	// ErrServerIndexOOB is returned in case
	// a given server index used is out of bounds,
	// for the cluster context it is used within.
	ErrServerIndexOOB = errors.New("server index out of bounds")
	// ErrInvalidInput is an error returned
	// when the input given for a function is invalid.
	ErrInvalidInput = errors.New("invalid input given")
)

/*
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
	serverIndex, err := ComputeServerIndex(cluster.serverCount, objectIndex, cluster.serverOperational)
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
	return ComputeServerIndex(cluster.serverCount, objectIndex, cluster.serverOperational)
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

*/
