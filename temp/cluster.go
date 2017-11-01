package temp

import (
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

// NewCluster creates a new Cluster
func NewCluster(serverCount int64, slave *Cluster) *Cluster {
	cluster := &Cluster{
		serverCount: serverCount,
		slave:       slave,
	}

	for i := int64(0); i < serverCount; i++ {
		cluster.servers = append(cluster.servers, config.StorageServerStateOnline)
	}

	return cluster
}

// Cluster ...
type Cluster struct {
	servers     []config.StorageServerState
	serverCount int64
	slave       *Cluster
}

type (
	// PrimaryIndex ...
	PrimaryIndex int64
	// SlaveIndex ...
	SlaveIndex int64
)

// ServerIndex returns the primary/slave index for a given object index
func (cluster *Cluster) ServerIndex(objectIndex int64) (interface{}, error) {
	if cluster == nil {
		return -1, storage.ErrClusterNotDefined
	}

	rawServerIndex := objectIndex % cluster.serverCount
	serverIndex, err := cluster.normalizeServerIndex(rawServerIndex, objectIndex)
	if err != nil || serverIndex != nil {
		return serverIndex, err // error or non-nil serverIndex gets returned early
	}

	// ensure we have online or offline servers
	if !cluster.isClusterAvailable() {
		return nil, ardb.ErrNoServersAvailable
	}

	var j int64
	var key uint64
	for {
		key = uint64(objectIndex)
		j = 0
		for j < cluster.serverCount {
			rawServerIndex = j
			key = key*2862933555777941757 + 1
			j = int64(float64(rawServerIndex+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
		}

		serverIndex, err = cluster.normalizeServerIndex(rawServerIndex, objectIndex)
		if err != nil || serverIndex != nil {
			return serverIndex, err // error or non-nil serverIndex gets returned early
		}

		objectIndex++
	}
}

// SetServerState sets the state for a given server.
func (cluster *Cluster) SetServerState(serverIndex int64, state config.StorageServerState) error {
	if serverIndex < 0 || serverIndex >= cluster.serverCount {
		return ardb.ErrServerIndexOOB
	}

	cluster.servers[serverIndex] = state
	return nil
}

func (cluster *Cluster) isClusterAvailable() bool {
	if cluster == nil {
		return false
	}

	var serverOfflineAvailable bool
	for _, state := range cluster.servers {
		if state == config.StorageServerStateOnline {
			return true
		}
		if state == config.StorageServerStateOffline {
			serverOfflineAvailable = true
		}
	}
	if !serverOfflineAvailable {
		return false
	}

	return cluster.slave.isClusterAvailable()
}

func (cluster *Cluster) normalizeServerIndex(serverIndex, objectIndex int64) (interface{}, error) {
	switch cluster.servers[serverIndex] {
	case config.StorageServerStateOnline:
		// return primary server
		return PrimaryIndex(serverIndex), nil

	case config.StorageServerStateRIP:
		return nil, nil

	case config.StorageServerStateOffline:
		// get slave server instead (if possible)
		index, err := cluster.slave.ServerIndex(objectIndex)
		if err != nil {
			if err == storage.ErrClusterNotDefined {
				return nil, ardb.ErrServerUnavailable
			}
			return nil, err
		}
		if index == nil {
			return nil, nil
		}
		return SlaveIndex(index.(PrimaryIndex)), nil

	default:
		// we should never get at this point,
		// while a server is respreading/reparing
		return nil, ardb.ErrServerUnavailable
	}
}

// const ...
const (
	InvalidServerIndex = int64(-1)
)
