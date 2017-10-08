package ardb

import (
	"errors"
	"sync"

	"github.com/zero-os/0-Disk/config"
)

// StorageCluster defines the interface of an
// object which allows you to interact with an ARDB Storage Cluster.
type StorageCluster interface {
	// Do applies a given action
	// to the first available server of this cluster.
	Do(action StorageAction) (reply interface{}, err error)
	// DoFor applies a given action
	// to a server within this cluster which maps to the given objectIndex.
	DoFor(objectIndex int64, action StorageAction) (reply interface{}, err error)
}

// NewCluster creates a new (ARDB) cluster.
func NewCluster(cfg *config.StorageClusterConfig, dialer ConnectionDialer) (*Cluster, error) {
	if dialer == nil {
		dialer = new(StandardConnectionDialer)
	}

	if cfg == nil {
		return &Cluster{dialer: dialer}, nil
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Cluster{
		servers:     cfg.Servers,
		serverCount: int64(len(cfg.Servers)),
		dialer:      dialer,
	}, nil
}

// ServerStateChangeHandler is a handler which allows
// you to react on ServerState changes,
// either because a single server gets updated or the entire cluster.
type ServerStateChangeHandler func(index int64, cfg config.StorageServerConfig, prevState config.StorageServerState) error

// Cluster defines the in memory cluster model for a single cluster.
// It can (and probably should) be used as a StorageCluster.
type Cluster struct {
	servers            []config.StorageServerConfig
	serverCount        int64
	serverStateHandler ServerStateChangeHandler
	dialer             ConnectionDialer
	mux                sync.RWMutex
}

// Do implements StorageCluster.Do
func (cluster *Cluster) Do(action StorageAction) (interface{}, error) {
	conn, err := cluster.connection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return action.Do(conn)
}

func (cluster *Cluster) connection() (Conn, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	for _, server := range cluster.servers {
		if server.State == config.StorageServerStateOnline {
			return cluster.dialer.Dial(server)
		}
	}

	return nil, ErrNoServersAvailable
}

// DoFor implements StorageCluster.DoFor
func (cluster *Cluster) DoFor(objectIndex int64, action StorageAction) (interface{}, error) {
	conn, err := cluster.connectionFor(objectIndex)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return action.Do(conn)
}

func (cluster *Cluster) connectionFor(objectIndex int64) (Conn, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	if cluster.serverCount == 0 {
		return nil, ErrNoServersAvailable
	}

	serverIndex, err := ComputeServerIndex(cluster.serverCount, objectIndex, cluster.serverIsOnline)
	if err != nil {
		return nil, err
	}

	server := cluster.servers[serverIndex]
	return cluster.dialer.Dial(server)
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
	defer cluster.mux.Unlock()

	if serverIndex < 0 || serverIndex > cluster.serverCount {
		return ErrServerIndexOOB
	}

	if cluster.serverStateHandler != nil {
		cfg := cluster.servers[serverIndex]
		prevState := cfg.State
		cfg.State = state

		err := cluster.serverStateHandler(serverIndex, cfg, prevState)
		if err != nil {
			return err
		}
		cluster.servers[serverIndex] = cfg
	} else {
		cluster.servers[serverIndex].State = state
	}

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
// It can (and probably should) be used as a StorageCluster.
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

// Do implements StorageCluster.Do
func (pair *ClusterPair) Do(action StorageAction) (interface{}, error) {
	conn, err := pair.connection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return action.Do(conn)
}

func (pair *ClusterPair) connection() (Conn, error) {
	pair.mux.RLock()
	defer pair.mux.RUnlock()

	var server config.StorageServerConfig
	for index := int64(0); index < pair.fstServerCount; index++ {
		// check if primary server is online
		server = pair.fstServers[index]
		if server.State == config.StorageServerStateOnline {
			return pair.dialer.Dial(server)
		}
		if server.State != config.StorageServerStateOffline || pair.sndServerCount == 0 {
			continue
		}

		// if slave servers are available, and the primary server state is offline,
		// let's try to get that slave server
		server = pair.sndServers[index]
		if server.State == config.StorageServerStateOnline {
			return pair.dialer.Dial(server)
		}
	}

	return nil, ErrNoServersAvailable
}

// DoFor implements StorageCluster.DoFor
func (pair *ClusterPair) DoFor(objectIndex int64, action StorageAction) (interface{}, error) {
	conn, err := pair.connectionFor(objectIndex)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return action.Do(conn)
}

func (pair *ClusterPair) connectionFor(objectIndex int64) (Conn, error) {
	pair.mux.RLock()
	defer pair.mux.RUnlock()

	serverIndex, err := ComputeServerIndex(pair.fstServerCount, objectIndex, pair.serverIsOnline)
	if err != nil {
		return nil, err
	}

	server := pair.fstServers[serverIndex]
	if server.State == config.StorageServerStateOnline {
		return pair.dialer.Dial(server)
	}

	server = pair.sndServers[serverIndex]
	return pair.dialer.Dial(server)
}

// SetPrimaryStorageConfig allows you to overwrite the currently used primary storage config
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

// SetSecondaryStorageConfig allows you to (un)set the currently used secondary storage config
func (pair *ClusterPair) SetSecondaryStorageConfig(cfg *config.StorageClusterConfig) error {
	if cfg == nil {
		// if cfg == nil,
		// we'll assume that the user wants to unset the secondary storage config,
		// this is fine as it is optional.
		pair.mux.Lock()
		pair.sndServers = nil
		pair.sndServerCount = 0
		pair.mux.Unlock()
		return nil
	}

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
	defer pair.mux.Unlock()

	if serverIndex < 0 || serverIndex > pair.fstServerCount {
		return ErrServerIndexOOB
	}

	if pair.fstServerStateHandler != nil {
		cfg := pair.fstServers[serverIndex]
		prevState := cfg.State
		cfg.State = state

		err := pair.fstServerStateHandler(serverIndex, cfg, prevState)
		if err != nil {
			return err
		}
		pair.fstServers[serverIndex] = cfg
	} else {
		pair.fstServers[serverIndex].State = state
	}

	return nil
}

// SetSecondaryServerState sets the state of a secondary server.
func (pair *ClusterPair) SetSecondaryServerState(serverIndex int64, state config.StorageServerState) error {
	pair.mux.Lock()
	defer pair.mux.Unlock()

	if serverIndex < 0 || serverIndex > pair.sndServerCount {
		return ErrServerIndexOOB
	}

	if pair.sndServerStateHandler != nil {
		cfg := pair.sndServers[serverIndex]
		prevState := cfg.State
		cfg.State = state

		err := pair.sndServerStateHandler(serverIndex, cfg, prevState)
		if err != nil {
			return err
		}
		pair.sndServers[serverIndex] = cfg
	} else {
		pair.sndServers[serverIndex].State = state
	}

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

// NopCluster is a Cluster which can be used for
// scenarios where you want to specify a StorageCluster,
// which only ever returns NoServersAvailable error.
type NopCluster struct{}

// Do implements StorageCluster.Do
func (cluster NopCluster) Do(action StorageAction) (reply interface{}, err error) {
	return nil, ErrNoServersAvailable
}

// DoFor implements StorageCluster.DoFor
func (cluster NopCluster) DoFor(objectIndex int64, action StorageAction) (reply interface{}, err error) {
	return nil, ErrNoServersAvailable
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

// enforces that our std StorageClusters
// are actually StorageClusters
var (
	_ StorageCluster = (*Cluster)(nil)
	_ StorageCluster = (*ClusterPair)(nil)
)

var (
	// ErrServerUnavailable is returned in case
	// a given server is unavailable (e.g. offline).
	ErrServerUnavailable = errors.New("server is unavailable")
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
