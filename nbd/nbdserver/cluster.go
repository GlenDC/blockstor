package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/lunny/log"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// nbdStorageCluster TODO
type nbdStorageCluster struct {
	primaryServers     []config.StorageServerConfig
	primaryServerCount int64

	slaveServers     []config.StorageServerConfig
	slaveServerCount int64

	vdiskID string

	pool *ardb.Pool

	mux sync.RWMutex
}

// Connection implements ardb.Cluster.Connection
func (cluster *nbdStorageCluster) Connection() (ardb.Conn, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	var server config.StorageServerConfig
	for index := range cluster.primaryServers {
		// check if primary server is online
		server = cluster.primaryServers[index]
		if server.State == config.StorageServerStateOnline {
			return cluster.pool.Dial(ConnConfig{
				Address:  server.Address,
				Database: server.Database,
			})
		}
		if server.State != config.StorageServerStateOffline || cluster.slaveServerCount == 0 {
			continue
		}

		// if slave servers are available, and the primary server state is offline,
		// let's try to get that slave server
		server = cluster.slaveServers[index]
		if server.State == config.StorageServerStateOnline {
			return cluster.pool.Dial(ConnConfig{
				Address:  server.Address,
				Database: server.Database,
			})
		}
	}

	return nil, ErrNoServersAvailable
}

// ConnectionFor implements ardb.Cluster.ConnectionFor
func (cluster *nbdStorageCluster) ConnectionFor(objectIndex int64) (ardb.Conn, error) {
	cluster.mux.RLock()
	defer cluster.mux.RUnlock()

	serverIndex, err := ComputeServerIndex(cluster.serverCount, objectIndex, cluster.serverIsOnline)
	if err != nil {
		return nil, err
	}

	server := cluster.primaryServers[serverIndex]
	if server.State == config.StorageServerStateOnline {
		return cluster.pool.Dial(ConnConfig{
			Address:  server.Address,
			Database: server.Database,
		})
	}

	server = cluster.slaveServers[serverIndex]
	return cluster.pool.Dial(ConnConfig{
		Address:  server.Address,
		Database: server.Database,
	})
}

func (cluster *nbdStorageCluster) Close() error {
	cluster.pool.Close()
}

func (cluster *nbdStorageCluster) spawnBackground(ctx context.Context, configSource config.Source) error {
	updateCh, err := config.WatchNBDStorageConfig(ctx, configSource, cluster.vdiskID)
	if err != nil {
		return err
	}

	var storageCfg config.NBDStorageConfig
	select {
	case <-ctx.Done():
		return fmt.Errorf(
			"couldn't get storage config for vdisk %s as context is done",
			cluster.vdiskID)
	case storageCfg = <-updateCh:
		cluster.updateConfig(storageCfg)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debugf("exit background process for vdisk %s's nbdStorageCluster", cluster.vdiskID)
				return

			case storageCfg = <-updateCh:
				cluster.mux.Lock()
				cluster.updateConfig(storageCfg)
				cluster.mux.Unlock()
			}
		}
	}()
}

func (cluster *nbdStorageCluster) updateConfig(cfg config.NBDStorageConfig) {
	cluster.primaryServers = cfg.StorageCluster.Servers
	cluster.primaryServerCount = int64(len(cluster.primaryServers))

	if cfg.SlaveStorageCluster == nil {
		cluster.slaveServers = nil
		cluster.slaveServerCount = 0
	} else {
		cluster.slaveServers = cfg.SlaveStorageCluster.Servers
		cluster.slaveServerCount = int64(len(cluster.slaveServers))
	}
}

// serverOperational returns true if
// a primary/slave server on the given index is online.
func (cluster *nbdStorageCluster) serverIsOnline(index int64) bool {
	server := cluster.primaryServers[index]
	if server.State == config.StorageServerStateOnline {
		return true
	}
	if server.State != config.StorageServerStateOffline || cluster.slaveServerCount == 0 {
		return false
	}

	server = cluster.slaveServers[index]
	return server.State == config.StorageServerStateOnline
}
