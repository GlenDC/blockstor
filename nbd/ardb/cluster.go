package ardb

import (
	"errors"
	"sync"

	"github.com/zero-os/0-Disk/config"
)

// ConnProvider blaa...
type ConnProvider interface {
	// Connection dials a connection
	// to the first available server of this provider.
	Connection() (Conn, error)
	// Connection dials a connection to a server within this provider,
	// and which currently maps to the given objectIndex.
	ConnectionFor(objectIndex int64) (Conn, error)
}

// TODO:
// ensure that *Cluster and *ClusterPair adhere to the ConnProvider interface type

// NewCluster creates a new (ARDB) cluster.
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

// ServerStateChangeHandler TODO...
type ServerStateChangeHandler func(index int64, cfg config.StorageServerConfig, prevState config.StorageServerState) error

// Cluster defines the in memory cluster
type Cluster struct {
	servers            []config.StorageServerConfig
	serverCount        int64
	serverStateHandler ServerStateChangeHandler
	dialer             ConnectionDialer
	mux                sync.RWMutex
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

// SetStorageconfig allows you to overwrite the currently used storage config
func (cluster *Cluster) SetStorageconfig(cfg config.StorageClusterConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	if cluster.serverStateHandler == nil {
		// if no state change handler is configured,
		// we can keep it simple and just update the servers used
		cluster.mux.Lock()
		cluster.servers = cfg.Servers
		cluster.serverCount = int64(len(cfg.Servers))
		cluster.mux.Unlock()
		return nil
	}

	// as a state change handler is registered
	// we need to make sure to handle each update
	cluster.mux.Lock()
	defer cluster.mux.Unlock()

	// handle each updated server,
	// that is: a server which only changed state

	serverCount := int64(len(cfg.Servers))
	if serverCount > cluster.serverCount {
		serverCount = cluster.serverCount
	}

	var err error
	var origServer, newServer config.StorageServerConfig

	for index := int64(0); index < serverCount; index++ {
		origServer, newServer = cluster.servers[index], cfg.Servers[index]
		if !storageServersEqual(origServer, newServer) || origServer.State == newServer.State {
			continue // a new server or non-changed state, so no update here
		}

		err = cluster.serverStateHandler(index, newServer, origServer.State)
		if err != nil {
			return err
		}
	}

	cluster.servers = cfg.Servers
	cluster.serverCount = serverCount
	return nil
}

func storageServersEqual(a, b config.StorageServerConfig) bool {
	return a.Database == b.Database &&
		a.Address == b.Address
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

// SetServerStateChangeHandler allows you to (un)set a handler
// which reacts on a server's state being updated.
func (cluster *Cluster) SetServerStateChangeHandler(handler ServerStateChangeHandler) {
	cluster.mux.Lock()
	cluster.serverStateHandler = handler
	cluster.mux.Unlock()
}

// serverOperational returns true if
// the server on the given index is online.
func (cluster *Cluster) serverIsOnline(index int64) bool {
	return cluster.servers[index].State == config.StorageServerStateOnline
}

// NewClusterPair creates a new (ARDB) cluster pair.
func NewClusterPair(first config.StorageClusterConfig, second *config.StorageClusterConfig, dialer ConnectionDialer) (*ClusterPair, error) {
	err := first.Validate()
	if err != nil {
		return nil, err
	}

	pair := &ClusterPair{
		fstServers:     first.Servers,
		fstServerCount: int64(len(first.Servers)),
		dialer:         dialer,
	}

	if second != nil {
		err = second.Validate()
		if err != nil {
			return nil, err
		}

		pair.sndServers = second.Servers
		pair.sndServerCount = int64(len(second.Servers))
	}

	if pair.dialer == nil {
		pair.dialer = new(StandardConnectionDialer)
	}

	return pair, nil
}

// ClusterPair defines a pair of clusters,
// where the secondary cluster is used as a backup for the first cluster.
type ClusterPair struct {
	fstServers            []config.StorageServerConfig
	fstServerCount        int64
	fstServerStateHandler ServerStateChangeHandler

	sndServers            []config.StorageServerConfig
	sndServerCount        int64
	sndServerStateHandler ServerStateChangeHandler

	dialer ConnectionDialer

	mux sync.RWMutex
}

// Connection implements ConnProvider.Connection
func (pair *ClusterPair) Connection() (Conn, error) {
	pair.mux.RLock()
	defer pair.mux.RUnlock()

	var server config.StorageServerConfig
	for index := int64(0); index < pair.fstServerCount; index++ {
		// check if primary server is online
		server = pair.fstServers[index]
		if server.State == config.StorageServerStateOnline {
			return pair.dialer.Dial(ConnConfig{
				Address:  server.Address,
				Database: server.Database,
			})
		}
		if server.State != config.StorageServerStateOffline || pair.sndServerCount == 0 {
			continue
		}

		// if slave servers are available, and the primary server state is offline,
		// let's try to get that slave server
		server = pair.sndServers[index]
		if server.State == config.StorageServerStateOnline {
			return pair.dialer.Dial(ConnConfig{
				Address:  server.Address,
				Database: server.Database,
			})
		}
	}

	return nil, ErrNoServersAvailable
}

// ConnectionFor implements ConnProvider.ConnectionFor
func (pair *ClusterPair) ConnectionFor(objectIndex int64) (Conn, error) {
	pair.mux.RLock()
	defer pair.mux.RUnlock()

	serverIndex, err := ComputeServerIndex(pair.fstServerCount, objectIndex, pair.serverIsOnline)
	if err != nil {
		return nil, err
	}

	server := pair.fstServers[serverIndex]
	if server.State == config.StorageServerStateOnline {
		return pair.dialer.Dial(ConnConfig{
			Address:  server.Address,
			Database: server.Database,
		})
	}

	server = pair.sndServers[serverIndex]
	return pair.dialer.Dial(ConnConfig{
		Address:  server.Address,
		Database: server.Database,
	})
}

// SetPrimaryStorageConfig allows you to overwrite the currently used storage config
func (pair *ClusterPair) SetPrimaryStorageConfig(cfg config.StorageClusterConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	if pair.fstServerStateHandler == nil {
		// if no state change handler is configured,
		// we can keep it simple and just update the servers used
		pair.mux.Lock()
		pair.fstServers = cfg.Servers
		pair.fstServerCount = int64(len(cfg.Servers))
		pair.mux.Unlock()
		return nil
	}

	// as a state change handler is registered
	// we need to make sure to handle each update
	pair.mux.Lock()
	defer pair.mux.Unlock()

	// handle each updated server,
	// that is: a server which only changed state

	serverCount := int64(len(cfg.Servers))
	if serverCount > pair.fstServerCount {
		serverCount = pair.fstServerCount
	}

	var err error
	var origServer, newServer config.StorageServerConfig

	for index := int64(0); index < serverCount; index++ {
		origServer, newServer = pair.fstServers[index], cfg.Servers[index]
		if !storageServersEqual(origServer, newServer) || origServer.State == newServer.State {
			continue // a new server or non-changed state, so no update here
		}

		err = pair.fstServerStateHandler(index, newServer, origServer.State)
		if err != nil {
			return err
		}
	}

	pair.fstServers = cfg.Servers
	pair.fstServerCount = serverCount
	return nil
}

// SetSecondaryStorageConfig allows you to overwrite the currently used storage config
func (pair *ClusterPair) SetSecondaryStorageConfig(cfg config.StorageClusterConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	if pair.sndServerStateHandler == nil {
		// if no state change handler is configured,
		// we can keep it simple and just update the servers used
		pair.mux.Lock()
		pair.sndServers = cfg.Servers
		pair.sndServerCount = int64(len(cfg.Servers))
		pair.mux.Unlock()
		return nil
	}

	// as a state change handler is registered
	// we need to make sure to handle each update
	pair.mux.Lock()
	defer pair.mux.Unlock()

	// handle each updated server,
	// that is: a server which only changed state

	serverCount := int64(len(cfg.Servers))
	if serverCount > pair.sndServerCount {
		serverCount = pair.sndServerCount
	}

	var err error
	var origServer, newServer config.StorageServerConfig

	for index := int64(0); index < serverCount; index++ {
		origServer, newServer = pair.sndServers[index], cfg.Servers[index]
		if !storageServersEqual(origServer, newServer) || origServer.State == newServer.State {
			continue // a new server or non-changed state, so no update here
		}

		err = pair.sndServerStateHandler(index, newServer, origServer.State)
		if err != nil {
			return err
		}
	}

	pair.sndServers = cfg.Servers
	pair.sndServerCount = serverCount
	return nil
}

// SetPrimaryServerState sets the state of a primary server.
func (pair *ClusterPair) SetPrimaryServerState(serverIndex int64, state config.StorageServerState) error {
	pair.mux.Lock()
	if serverIndex < 0 || serverIndex > pair.fstServerCount {
		pair.mux.Unlock()
		return ErrServerIndexOOB
	}

	pair.fstServers[serverIndex].State = state
	pair.mux.Unlock()
	return nil
}

// SetSecondaryServerState sets the state of a secondary server.
func (pair *ClusterPair) SetSecondaryServerState(serverIndex int64, state config.StorageServerState) error {
	pair.mux.Lock()
	if serverIndex < 0 || serverIndex > pair.sndServerCount {
		pair.mux.Unlock()
		return ErrServerIndexOOB
	}

	pair.sndServers[serverIndex].State = state
	pair.mux.Unlock()
	return nil
}

// SetPrimaryServerStateChangeHandler allows you to (un)set a handler
// which reacts on a primary server's state being updated.
func (pair *ClusterPair) SetPrimaryServerStateChangeHandler(handler ServerStateChangeHandler) {
	pair.mux.Lock()
	pair.fstServerStateHandler = handler
	pair.mux.Unlock()
}

// SetSecondaryServerStateChangeHandler allows you to (un)set a handler
// which reacts on a secondary server's state being updated.
func (pair *ClusterPair) SetSecondaryServerStateChangeHandler(handler ServerStateChangeHandler) {
	pair.mux.Lock()
	pair.sndServerStateHandler = handler
	pair.mux.Unlock()
}

// serverOperational returns true if
// a server on the given index is online.
func (pair *ClusterPair) serverIsOnline(index int64) bool {
	server := pair.fstServers[index]
	if server.State == config.StorageServerStateOnline {
		return true
	}
	if server.State != config.StorageServerStateOffline || pair.sndServerCount == 0 {
		return false
	}

	server = pair.sndServers[index]
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
