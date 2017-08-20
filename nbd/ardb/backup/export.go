package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"

	"github.com/zero-os/0-Disk"
)

// Export a block storage to aa FTP Server,
// in a secure and space efficient manner,
// in order to provide a backup (snapshot) for later usage.
func Export(ctx context.Context, cfg Config) error {
	err := cfg.validate()
	if err != nil {
		return err
	}

	storageConfig, err := createBlockStorage(cfg.VdiskID, cfg.BlockStorageConfig, true)
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

	storageDriver, err := NewStorageDriver(cfg.BackupStorageConfig)
	if err != nil {
		return err
	}
	defer storageDriver.Close()

	exportConfig := exportConfig{
		JobCount:        cfg.JobCount,
		SrcBlockSize:    storageConfig.BlockStorage.BlockSize,
		DstBlockSize:    cfg.BlockSize,
		CompressionType: cfg.CompressionType,
		CryptoKey:       cfg.CryptoKey,
		SnapshotID:      cfg.SnapshotID,
		Force:           cfg.Force,
	}

	return exportBS(ctx, blockStorage, storageConfig.Indices, storageDriver, exportConfig)
}

func exportBS(ctx context.Context, src storage.BlockStorage, blockIndices []int64, dst StorageDriver, cfg exportConfig) error {
	// load the deduped map, or create a new one if it doesn't exist yet
	dedupedMap, err := ExistingOrNewDedupedMap(
		cfg.SnapshotID, dst, &cfg.CryptoKey, cfg.CompressionType, cfg.Force)
	if err != nil {
		return err
	}

	// setup the context that we'll use for all worker goroutines
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	inputCh := make(chan blockIndexPair, cfg.JobCount) // gets closed by fetcher goroutine

	errCh := make(chan error)
	defer close(errCh)

	sendErr := func(err error) {
		log.Errorf("an error occured while exporting: %v", err)
		select {
		case errCh <- err:
		default:
		}
	}

	var exportErr error
	// err ch used to
	go func() {
		select {
		case <-ctx.Done():
		case exportErr = <-errCh:
			cancel() // stop all other goroutines
		}
	}()

	var wg sync.WaitGroup

	// launch fetcher, so it can start fetching blocks
	go func() {
		defer close(inputCh)

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

		defer func() {
			if err != nil {
				return
			}

			// if no error has yet occured,
			// ensure that at the end of this function,
			// the block fetcher is empty
			_, err = bf.FetchBlock()
			if err == nil || err != io.EOF {
				err = errors.New("storage's block fetcher still has unstored content left")
				sendErr(err)
				return
			}
			err = nil
		}()

		var inPair *blockIndexPair

		// keep fetching blocks,
		// until we received an error,
		// where the error hopefully is just io.EOF
		for {
			select {
			case <-ctx.Done():
				return

			default:
				// fetch the next available block
				inPair, err = bf.FetchBlock()
				if err != nil {
					if err == io.EOF {
						err = nil
					} else {
						sendErr(fmt.Errorf("error while fetching block: %v", err))
					}
					return
				}

				select {
				case <-ctx.Done():
					return
				case inputCh <- *inPair:
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
			Compressor:    compressor,
			Encrypter:     encrypter,
			StorageDriver: dst,
			DedupedMap:    dedupedMap,
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

			var input blockIndexPair
			var open bool

			for {
				select {
				case <-ctx.Done():
					return

				case input, open = <-inputCh:
					if !open {
						return
					}

					err = pipeline.WriteBlock(input.Index, input.Block)
					if err != nil {
						sendErr(fmt.Errorf("error while processing block: %v", err))
						return
					}
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
	pair.Block, err = sbf.storage.GetBlock(sbf.indices[sbf.index])
	if err != nil {
		return nil, err
	}
	pair.Index = sbf.indices[sbf.index]
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

	Force bool
}

// compress -> encrypt -> store
type exportPipeline struct {
	Compressor    Compressor
	Encrypter     Encrypter
	StorageDriver StorageDriver
	DedupedMap    *DedupedMap
}

func (p *exportPipeline) WriteBlock(index int64, data []byte) error {
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

	hash := zerodisk.HashBytes(bufA.Bytes())
	blockIsNew := p.DedupedMap.SetHash(index, hash)
	if !blockIsNew {
		return nil // we're done here
	}

	return p.StorageDriver.SetDedupedBlock(hash, bufA)
}
