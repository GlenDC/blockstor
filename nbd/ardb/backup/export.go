package backup

import (
	"bytes"
	"context"
	"errors"

	"github.com/zero-os/0-Disk/log"
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

func fetchStorageBlocks(ctx context.Context, indices []int64, storage storage.BlockStorage, srcSize, dstSize int64, bsize int) (<-chan []byte, <-chan error) {
	outputCh := make(chan []byte, bsize)
	errCh := make(chan error, bsize)

	go func() {
		log.Debug("starting fetch storage block goroutine")
		defer log.Debug("stopping fetch storage block goroutine")
		defer close(outputCh)
		defer close(errCh)

		nilBlock := make([]byte, srcSize)

		previousIndex := indices[0]
		var targetBlock []byte

		for _, index := range indices {
			select {
			case <-ctx.Done():
				return

			default:
				block, err := fetchStorageBlock(storage, index, srcSize)
				if err != nil {
					errCh <- err
					block = nilBlock
				}

				for int64(len(block)) > dstSize {
					outputCh <- block[:dstSize]
					block = block[dstSize:]
				}

				if len(block) == 0 {
					previousIndex = index
					continue
				}

				// TODO:
				// Finish this off,
				// make sure to be able to:
				// + add block to targetBlock in case we still have left over
				// + make sure sure to be able to pad with zero's,
				//   in case the current index is not the index right after the previous index
				if previousIndex-index > 2 {
				}
				// ...

				// todo:
				outputCh <- targetBlock
			}
		}
	}()

	return outputCh, errCh
}

func fetchStorageBlock(storage storage.BlockStorage, blockIndex, blockSize int64) ([]byte, error) {
	content, err := storage.GetBlock(blockIndex)
	if err != nil {
		return nil, err
	}

	if int64(len(content)) < blockSize {
		block := make([]byte, blockSize)
		copy(block, content)
		content = block
	}

	return content, nil
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
