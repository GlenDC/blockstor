package lba

import (
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
)

// SectorStorage defines the API for a persistent storage,
// used to fetch sectors from which aren't cached yet,
// and to store sectors which are evicted from a cache.
// NOTE: a sectorStorage is not guaranteed to be thread-safe!
type SectorStorage interface {
	// GetSector fetches a sector from a storage,
	// returning an error if this wasn't possible.
	GetSector(index int64) (*Sector, error)

	// SetSector marks a sector persistent,
	// by preparing it to store on a stoage.
	// NOTE: the sectors won't be stored until you called a succesfull Flush!
	SetSector(index int64, sector *Sector)

	// Flush flushes all added sectors to the storage.
	// Essentially storing all sectors which have previously be set,
	// but have not yet been flushed.
	Flush() error
}

// ARDBSectorStorage creates a new sector storage
// which writes/reads to/from an ARDB Cluster
func ARDBSectorStorage(vdiskID string, cluster ardb.StorageCluster) SectorStorage {
	return &ardbSectorStorage{
		cluster: cluster,
		vdiskID: vdiskID,
		key:     StorageKey(vdiskID),
		cache:   make(map[int64][]ardb.StorageAction),
	}
}

// ardbSectorStorage is the sector storage implementation,
// used in production, and which writes/reads to/from an ARDB server.
type ardbSectorStorage struct {
	cluster      ardb.StorageCluster
	vdiskID, key string

	cache map[int64][]ardb.StorageAction
}

// GetSector implements sectorStorage.GetSector
func (s *ardbSectorStorage) GetSector(index int64) (*Sector, error) {
	reply, err := s.cluster.DoFor(index, ardb.Command(command.HashGet, s.key, index))
	if reply == nil {
		return NewSector(), nil
	}

	data, err := ardb.Bytes(reply, err)
	if err != nil {
		return nil, err
	}

	return SectorFromBytes(data)
}

// SetSector implements sectorStorage.SetSector
func (s *ardbSectorStorage) SetSector(index int64, sector *Sector) {
	var cmd *ardb.StorageCommand
	if data := sector.Bytes(); data == nil {
		cmd = ardb.Command(command.HashDelete, s.key, index)
	} else {
		cmd = ardb.Command(command.HashSet, s.key, index, data)
	}

	s.cache[index] = append(s.cache[index], cmd)
}

// Flush implements sectorStorage.Flush
func (s *ardbSectorStorage) Flush() error {
	if len(s.cache) == 0 {
		return nil // nothing to do
	}

	var err error
	var errors flushError

	// store all sectors, server per server
	for index, cmds := range s.cache {
		_, err = s.cluster.DoFor(index, ardb.Commands(cmds...))
		errors.AddError(err)
	}

	// return all errors that occured, if any
	return errors.AsError()
}
