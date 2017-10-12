package ardb

import (
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/redisstub/ledisdb"
)

func cluster() StorageCluster {
	server := ledisdb.NewServer()
	cluster, err := NewUniCluster(config.StorageServerConfig{
		Address: server.Address(),
	}, nil)
	if err != nil {
		panic(err)
	}
	return cluster
}

// TODO (part of https://github.com/zero-os/0-Disk/issues/543)
// add examples:
// ExampleNewCluster
// ExampleNewClusterPair
