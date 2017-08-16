package backup

import (
	"bytes"
	"errors"
	"io"

	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"

	"github.com/zero-os/0-Disk"
)

func Export(cfg Config) error {
	err := cfg.Validate()
	if err != nil {
		return err
	}

	storageConfig, err := createBlockStorage(cfg.VdiskID, cfg.StorageSource)
	if err != nil {
		return err
	}

	ardbProvider, err := ardb.StaticProvider(storageConfig.NBD, nil)
	if err != nil {
		return err
	}
	defer ardbProvider.Close()

	blockStorage, err := storage.NewBlockStorage(storageConfig.BlockStorage, ardbProvider)
	if err != nil {
		return err
	}
	defer blockStorage.Close()

	ftpDriver, err := FTPDriver(cfg.FTPServer)
	if err != nil {
		return err
	}
	defer ftpDriver.Close()

	exportConfig := exportConfig{
		JobCount:        cfg.JobCount,
		SrcBlockSize:    storageConfig.BlockStorage.BlockSize,
		DstBlockSize:    cfg.BlockSize,
		CompressionType: cfg.CompressionType,
		CryptoKey:       cfg.CryptoKey,
	}

	return export(blockStorage, storageConfig.Indices, ftpDriver, exportConfig)
}

func export(src storage.BlockStorage, blockIndices []int64, dst ServerDriver, cfg exportConfig) error {
	return errors.New("TODO")
}

func newStorageBlockFetcher(storage storage.BlockStorage, indices []int64, blockSize int64) *storageBlockFetcher {
	return &storageBlockFetcher{
		storage:   storage,
		indices:   indices,
		index:     0,
		length:    int64(len(indices)),
		blockSize: blockSize,
	}
}

type storageBlockFetcher struct {
	storage   storage.BlockStorage
	indices   []int64
	index     int64
	length    int64
	blockSize int64
}

func (sbf *storageBlockFetcher) FetchBlock() ([]byte, error) {
	if sbf.index >= sbf.length {
		return nil, io.EOF
	}

	block, err := sbf.storage.GetBlock(sbf.index)
	if err != nil {
		return nil, err
	}
	sbf.index++

	// a storage might return a block not big enough
	if int64(len(block)) < sbf.blockSize {
		nb := make([]byte, sbf.blockSize)
		copy(nb, block)
		block = nb
	}

	return block, nil
}

type exportConfig struct {
	JobCount int

	SrcBlockSize int64
	DstBlockSize int64

	CompressionType CompressionType
	CryptoKey       CryptoKey
}

// compress -> encrypt -> store
type exportPipeline struct {
	Compressor   Compressor
	Encrypter    Encrypter
	ServerDriver ServerDriver
}

func (p *exportPipeline) WriteBlock(hash zerodisk.Hash, data []byte) error {
	bufA := bytes.NewBuffer(data)
	bufB := bytes.NewBuffer(nil)

	err := p.Compressor.Compress(bufA, bufB)
	if err != nil {
		return err
	}

	bufA = bytes.NewBuffer(nil)
	err = p.Encrypter.Encrypt(bufB, bufA)
	if err != nil {
		return err
	}

	return p.ServerDriver.SetDedupedBlock(hash, bufA)
}
