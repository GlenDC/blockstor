package ardb

import (
	"errors"

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

// NewUniCluster creates a new (ARDB) uni-cluster.
// See `UniCluster` for more information.
func NewUniCluster(cfg config.StorageServerConfig, dialer ConnectionDialer) (*UniCluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if dialer == nil {
		dialer = stdConnDialer
	}

	return &UniCluster{
		server: cfg,
		dialer: dialer,
	}, nil
}

// UniCluster defines an in memory cluster model for a uni-cluster.
// Meaning it is a cluster with just /one/ ARDB server configured.
// As a consequence all methods will dial connections to this one server.
// This cluster type should only very be used for very specialized purposes.
type UniCluster struct {
	server config.StorageServerConfig
	dialer ConnectionDialer
}

// Do implements StorageCluster.Do
func (cluster *UniCluster) Do(action StorageAction) (interface{}, error) {
	// establish a connection for that serverIndex
	conn, err := cluster.dialer.Dial(cluster.server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// apply the given action to the established connection
	return action.Do(conn)
}

// DoFor implements StorageCluster.DoFor
func (cluster *UniCluster) DoFor(_ int64, action StorageAction) (interface{}, error) {
	// establish a connection for that serverIndex
	conn, err := cluster.dialer.Dial(cluster.server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// apply the given action to the established connection
	return action.Do(conn)
}

// NewCluster creates a new (ARDB) cluster.
func NewCluster(cfg config.StorageClusterConfig, dialer ConnectionDialer) (*Cluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if dialer == nil {
		dialer = stdConnDialer
	}

	return &Cluster{
		servers:     cfg.Servers,
		serverCount: int64(len(cfg.Servers)),
		dialer:      dialer,
	}, nil
}

// Cluster defines the in memory cluster model for a single cluster.
type Cluster struct {
	servers     []config.StorageServerConfig
	serverCount int64

	dialer ConnectionDialer
}

// Do implements StorageCluster.Do
func (cluster *Cluster) Do(action StorageAction) (interface{}, error) {
	// compute server index of first available server
	serverIndex, err := FindFirstServerIndex(cluster.serverCount, cluster.serverIsOnline)
	if err != nil {
		return nil, err
	}

	// establish a connection for that serverIndex
	conn, err := cluster.dialer.Dial(cluster.servers[serverIndex])
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// apply the given action to the established connection
	return action.Do(conn)
}

// DoFor implements StorageCluster.DoFor
func (cluster *Cluster) DoFor(objectIndex int64, action StorageAction) (interface{}, error) {
	// compute server index which maps to the given object index
	serverIndex, err := ComputeServerIndex(cluster.serverCount, objectIndex, cluster.serverIsOnline)
	if err != nil {
		return nil, err
	}

	// establish a connection for that serverIndex
	conn, err := cluster.dialer.Dial(cluster.servers[serverIndex])
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// apply the given action to the established connection
	return action.Do(conn)
}

// serverOperational returns true if
// the server on the given index is online.
func (cluster *Cluster) serverIsOnline(index int64) bool {
	return cluster.servers[index].State == config.StorageServerStateOnline
}

// NewClusterPair creates a new (ARDB) cluster pair.
func NewClusterPair(first, second config.StorageClusterConfig, dialer ConnectionDialer) (*ClusterPair, error) {
	err := first.Validate()
	if err != nil {
		return nil, err
	}
	err = second.Validate()
	if err != nil {
		return nil, err
	}

	// even though the first and second cluster on their own are proven to be valid,
	// we still need to ensure that their server count is equal
	fstServerCount := int64(len(first.Servers))
	sndServerCount := int64(len(second.Servers))
	if fstServerCount != sndServerCount {
		return nil, ErrIncompatibleServers
	}

	// default the dialer to a non-pooled redigo dialer
	if dialer == nil {
		dialer = stdConnDialer
	}

	// all fine, return the cluster pair,
	// ready for usage
	return &ClusterPair{
		fstServers:     first.Servers,
		fstServerCount: fstServerCount,
		sndServers:     second.Servers,
		sndServerCount: sndServerCount,
		dialer:         dialer,
	}, nil
}

// ClusterPair defines a pair of clusters,
// where the secondary cluster is used as a backup for the first cluster.
type ClusterPair struct {
	fstServers     []config.StorageServerConfig
	fstServerCount int64

	sndServers     []config.StorageServerConfig
	sndServerCount int64

	dialer ConnectionDialer
}

// Do implements StorageCluster.Do
func (pair *ClusterPair) Do(action StorageAction) (interface{}, error) {
	// compute server index of first available server
	serverIndex, err := FindFirstServerIndex(pair.fstServerCount, pair.serverIsOnline)
	if err != nil {
		return nil, err
	}

	// get server for the computed index
	server := pair.fstServers[serverIndex]
	if server.State != config.StorageServerStateOnline {
		server = pair.sndServers[serverIndex]
	}

	// establish a connection for that serverIndex
	conn, err := pair.dialer.Dial(server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// apply the given action to the established connection
	return action.Do(conn)
}

// DoFor implements StorageCluster.DoFor
func (pair *ClusterPair) DoFor(objectIndex int64, action StorageAction) (interface{}, error) {
	// compute server index for the server which maps to this objectIndex
	serverIndex, err := ComputeServerIndex(pair.fstServerCount, objectIndex, pair.serverIsOnline)
	if err != nil {
		return nil, err
	}

	// get server for the computed index
	server := pair.fstServers[serverIndex]
	if server.State != config.StorageServerStateOnline {
		server = pair.sndServers[serverIndex]
	}

	// establish a connection for that serverIndex
	conn, err := pair.dialer.Dial(server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// apply the given action to the established connection
	return action.Do(conn)
}

// serverOperational returns true if
// a server on the given index is online.
func (pair *ClusterPair) serverIsOnline(index int64) bool {
	server := pair.fstServers[index]
	if server.State == config.StorageServerStateOnline {
		return true
	}
	if server.State != config.StorageServerStateOffline {
		// only use second cluster
		// if server from first cluster is /only/ temporarly offline
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

// ServerIndexPredicate is a predicate
// used to determine if a given serverIndex
// for a callee-owned cluster object is valid.
type ServerIndexPredicate func(serverIndex int64) bool

// FindFirstAvailableServerConfig iterates through all storage servers
// until it finds a server which state indicates its available.
// If no such server exists, ErrNoServersAvailable is returned.
func FindFirstAvailableServerConfig(cfg config.StorageClusterConfig) (serverCfg config.StorageServerConfig, err error) {
	for _, serverCfg = range cfg.Servers {
		if serverCfg.State == config.StorageServerStateOnline {
			return
		}
	}

	err = ErrNoServersAvailable
	return
}

// FindFirstServerIndex iterates through all servers
// until the predicate for a server index returns true.
// If no index evaluates to true, ErrNoServersAvailable is returned.
func FindFirstServerIndex(serverCount int64, predicate ServerIndexPredicate) (int64, error) {
	for serverIndex := int64(0); serverIndex < serverCount; serverIndex++ {
		if predicate(serverIndex) {
			return serverIndex, nil
		}
	}

	return -1, ErrNoServersAvailable
}

// ComputeServerIndex computes a server index for a given objectIndex,
// using a shared static algorithm with the serverCount as input and
// a given predicate to define if a computed index is fine.
func ComputeServerIndex(serverCount, objectIndex int64, predicate ServerIndexPredicate) (int64, error) {
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
	stdConnDialer = new(StandardConnectionDialer)
)

// enforces that our std StorageClusters
// are actually StorageClusters
var (
	_ StorageCluster = (*UniCluster)(nil)
	_ StorageCluster = (*Cluster)(nil)
	_ StorageCluster = (*ClusterPair)(nil)
	_ StorageCluster = NopCluster{}
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

	// ErrIncompatibleServers is an error returned
	// in case 2 clusters used for a ClusterPair are not
	// compatible with each other (e.g. due to servers not being equal).
	ErrIncompatibleServers = errors.New("incompatible servers (server count equal?)")
)
