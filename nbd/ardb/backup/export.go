package backup

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/lunny/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"

	"github.com/zero-os/0-Disk"
)

// Export a block storage to an FTP Server,
// in a secure and space efficient manner,
// in order to provide a backup (snapshot) for later usage.
func Export(ctx context.Context, cfg Config) error {
	err := cfg.validate()
	if err != nil {
		return err
	}

	storageConfig, err := createBlockStorage(cfg.VdiskID, cfg.StorageSource, true)
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
		SnapshotID:      cfg.SnapshotID,
	}

	return export(ctx, blockStorage, storageConfig.Indices, ftpDriver, exportConfig)
}

func export(ctx context.Context, src storage.BlockStorage, blockIndices []int64, dst ServerDriver, cfg exportConfig) error {
	// load the deduped map, or create a new one if it doesn't exist yet
	dedupedMap, err := ExistingOrNewDedupedMap(
		cfg.SnapshotID, dst, &cfg.CryptoKey, cfg.CompressionType)
	if err != nil {
		return err
	}

	// setup the context that we'll use for all worker goroutines
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	inputCh := make(chan blockHashPair, cfg.JobCount*2) // gets closed by fetcher goroutine
	errCh := make(chan error)                           // gets closed by error handling goroutine

	var exportErr error
	// err ch used to
	go func() {
		defer close(errCh)
		select {
		case <-ctx.Done():
		case exportErr = <-errCh:
			cancel() // stop all other goroutines
		}
	}()

	var wg sync.WaitGroup

	// launch fetcher, so it can start fetcing blocks
	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Debug("starting export's block fetcher")

		var err error
		defer func() {
			if err != nil {
				log.Debugf("stopping export's block fetcher with error: %v", err)
				return
			}
			log.Debug("stopping export's block fetcher")
		}()

		// setup the block fetcher for the source
		sbf := newStorageBlockFetcher(src, blockIndices, cfg.SrcBlockSize)
		bf := sizedBlockFetcher(sbf, cfg.SrcBlockSize, cfg.DstBlockSize)

		var blockHasChanged bool
		var hash zerodisk.Hash
		var pair *blockIndexPair

		// keep fetching blocks,
		// until we received an error,
		// where the error hopefully is just io.EOF
		for {
			select {
			case <-ctx.Done():
				return

			default:
				// fetch the next available block
				pair, err = bf.FetchBlock()
				if err != nil {
					if err != io.EOF {
						errCh <- err
						err = nil
					}
					return
				}

				hash = zerodisk.HashBytes(pair.Block)
				blockHasChanged = dedupedMap.SetHash(pair.Index, hash)
				if !blockHasChanged {
					log.Debugf("block %d already existed, so skipping its serialization", pair.Index)
					continue // no need to serialize the block itself
					// as it already existed
				}

				inputCh <- blockHashPair{
					Block: pair.Block,
					Hash:  hash,
				}
			}
		}
	}()

	// launch all workers
	wg.Add(cfg.JobCount)
	for i := 0; i < cfg.JobCount; i++ {
		compressor, err := NewCompressor(cfg.CompressionType)
		if err != nil {
			return err
		}
		encrypter, err := NewEncrypter(&cfg.CryptoKey)
		if err != nil {
			return err
		}

		pipeline := &exportPipeline{
			Compressor:   compressor,
			Encrypter:    encrypter,
			ServerDriver: dst,
		}

		// launch worker
		go func(id int) {
			defer wg.Done()

			log.Debugf("starting export worker #%d", id)

			var err error
			defer func() {
				if err != nil {
					log.Debugf("stopping export worker #%d with error: %v", id, err)
					return
				}
				log.Debugf("stopping export worker #%d", id)
			}()

			for {
				select {
				case <-ctx.Done():
					return

				case pair := <-inputCh:
					err = pipeline.WriteBlock(pair.Hash, pair.Block)
					errCh <- err
					return
				}
			}
		}(i)
	}

	// wait until all blocks have been fetched and backed up
	wg.Wait()

	// check if error was thrown, if so, quit with an error immediately
	if exportErr != nil {
		return exportErr
	}

	// store the deduped map
	buf := bytes.NewBuffer(nil)
	err = dedupedMap.Serialize(&cfg.CryptoKey, cfg.CompressionType, buf)
	if err != nil {
		return err
	}
	return dst.SetDedupedMap(cfg.SnapshotID, buf)
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

// FetchBlock implements blockFetcher.FetchBlock
func (sbf *storageBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	if sbf.index >= sbf.length {
		return nil, io.EOF
	}

	var err error
	pair := new(blockIndexPair)
	pair.Block, err = sbf.storage.GetBlock(sbf.index)
	if err != nil {
		return nil, err
	}
	pair.Index = sbf.index
	sbf.index++

	// a storage might return a block not big enough
	if int64(len(pair.Block)) < sbf.blockSize {
		nb := make([]byte, sbf.blockSize)
		copy(nb, pair.Block)
		pair.Block = nb
	}

	return pair, nil
}

type exportConfig struct {
	JobCount int

	SrcBlockSize int64
	DstBlockSize int64

	CompressionType CompressionType
	CryptoKey       CryptoKey

	SnapshotID string
}

type blockHashPair struct {
	Hash  zerodisk.Hash
	Block []byte
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
