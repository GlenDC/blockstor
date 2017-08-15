package backup

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/zero-os/0-Disk/nbd/ardb"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

type Config struct {
	// Required: VdiskID to export from or import into
	VdiskID string
	// Optional: ID of the snapshot (same as VdiskID by default)
	SnapshotID string

	// Optional: Snapshot BlockSize (128KiB by default)
	BlockSize int64

	// Required: SourceConfig to configure the storage with
	StorageSource config.SourceConfig
	// Required: FTPServerConfig used to configure the destination FTP Server
	FTPServer FTPServerConfig

	// Optional: Amount of jobs (goroutines) to run simultaneously
	//           (to import/export in parallel)
	JobCount int

	// Type of Compression to use for compressing/decompressing.
	// Note: this should be the same value for an import/export pair
	CompressionType CompressionType
	// CryptoKey to use for encryption/decryption.
	// Note: this should be the same value for an import/export pair
	CryptoKey CryptoKey
}

func (cfg *Config) Validate() error {
	if cfg.VdiskID == "" {
		return errNilVdiskID
	}
	if cfg.SnapshotID == "" {
		cfg.SnapshotID = cfg.VdiskID
	}

	if x := cfg.BlockSize; x < 512 || (x&(x-1)) != 0 {
		return fmt.Errorf(
			"blockSize '%d' is not a power of 2 or smaller then 512", cfg.BlockSize)
	}

	err := cfg.StorageSource.Validate()
	if err != nil {
		return err
	}

	err = cfg.FTPServer.Validate()
	if err != nil {
		return err
	}

	if cfg.JobCount <= 0 {
		cfg.JobCount = runtime.NumCPU() * 2
	}

	err = cfg.CompressionType.Validate()
	if err != nil {
		return err
	}

	return nil
}

type storageConfig struct {
	Indices      []int64
	NBD          config.NBDStorageConfig
	BlockStorage storage.BlockStorageConfig
}

func createBlockStorage(vdiskID string, sourceConfig config.SourceConfig) (*storageConfig, error) {
	storageConfigCloser, err := config.NewSource(sourceConfig)
	if err != nil {
		return nil, err
	}
	defer storageConfigCloser.Close()

	vdiskConfig, err := config.ReadVdiskStaticConfig(storageConfigCloser, vdiskID)
	if err != nil {
		return nil, err
	}

	nbdStorageConfig, err := config.ReadNBDStorageConfig(storageConfigCloser, vdiskID, vdiskConfig)
	if err != nil {
		return nil, err
	}

	indices, err := storage.ListBlockIndices(vdiskID, vdiskConfig.Type, &nbdStorageConfig.StorageCluster)
	if err != nil {
		return nil, err
	}

	blockStorage := storage.BlockStorageConfig{
		VdiskID:         vdiskID,
		TemplateVdiskID: vdiskConfig.TemplateVdiskID,
		VdiskType:       vdiskConfig.Type,
		BlockSize:       int64(vdiskConfig.BlockSize),
		LBACacheLimit:   ardb.DefaultLBACacheLimit,
	}

	return &storageConfig{
		Indices:      indices,
		NBD:          *nbdStorageConfig,
		BlockStorage: blockStorage,
	}, nil
}

var (
	errNilVdiskID       = errors.New("vdisk's identifier not given")
	errInvalidCryptoKey = errors.New("invalid crypto key")
)
