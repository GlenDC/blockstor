package main

import (
	"errors"

	"github.com/zero-os/0-Disk/nbd/ardb"
)

// nbdStorageCluster TODO
type nbdStorageCluster struct {
}

// Connection implements ardb.Cluster.Connection
func (cluster *nbdStorageCluster) Connection() (ardb.Conn, error) {
	return nil, errors.New("TODO")
}

// ConnectionFor implements ardb.Cluster.ConnectionFor
func (cluster *nbdStorageCluster) ConnectionFor(objectIndex int64) (ardb.Conn, error) {
	return nil, errors.New("TODO")
}
