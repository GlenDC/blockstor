package backup

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

func TestStorageBlockFetcher(t *testing.T) {
	assert := assert.New(t)

	const (
		vdiskID    = "a"
		blockCount = 8
		blockSize  = 8
	)

	storage := storage.NewInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(storage) {
		return
	}

	sourceData := make([]byte, blockSize*blockCount)
	rand.Read(sourceData)

	var indices []int64
	for i := 0; i < blockCount; i++ {
		start := i * int(blockSize)
		end := start + int(blockSize)
		index := int64(i)

		if i%2 == 1 {
			newEnd := end - int(blockSize)/2
			copy(sourceData[newEnd:end], make([]byte, end-newEnd+1))
			end = newEnd
		}

		err := storage.SetBlock(index, sourceData[start:end])
		if !assert.NoError(err) {
			return
		}

		// validate the block was correctly set
		block, err := storage.GetBlock(index)
		if !assert.NoError(err) {
			return
		}
		if !assert.Equalf(sourceData[start:end], block, "block: %v", i) {
			return
		}

		indices = append(indices, index)
	}

	// test fetcher with all blocks

	fetcher := newStorageBlockFetcher(storage, indices, blockSize)
	if !assert.NotNil(fetcher) {
		return
	}

	for i := 0; i < blockCount; i++ {
		block, err := fetcher.FetchBlock()
		if assert.NoError(err) {
			start := i * int(blockSize)
			end := start + int(blockSize)

			assert.Equalf(sourceData[start:end], block, "block: %v", i)
		}
	}

	_, err := fetcher.FetchBlock()
	assert.Equal(io.EOF, err)

	// test fetcher with some blocks

	maxCount := blockCount / 2

	fetcher = newStorageBlockFetcher(storage, indices[:maxCount], blockSize)
	if !assert.NotNil(fetcher) {
		return
	}

	for i := 0; i < maxCount; i++ {
		block, err := fetcher.FetchBlock()
		if assert.NoError(err) {
			start := i * int(blockSize)
			end := start + int(blockSize)

			assert.Equalf(sourceData[start:end], block, "block: %v", i)
		}
	}

	_, err = fetcher.FetchBlock()
	assert.Equal(io.EOF, err)
}
