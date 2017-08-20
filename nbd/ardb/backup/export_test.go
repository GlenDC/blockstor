package backup

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

func TestStorageBlockFetcher_o0_i1(t *testing.T) {
	testStorageBlockFetcher(t, 0, 1)
}

func TestStorageBlockFetcher_o1_i1(t *testing.T) {
	testStorageBlockFetcher(t, 1, 1)
}

func TestStorageBlockFetcher_o4_i9(t *testing.T) {
	testStorageBlockFetcher(t, 4, 9)
}

func TestStorageBlockFetcher_o27_i101(t *testing.T) {
	testStorageBlockFetcher(t, 27, 101)
}

func testStorageBlockFetcher(t *testing.T, offset, interval int64) {
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

	sourceData := generateSequentialDataBlock(0, blockSize*blockCount)

	var indices []int64
	for i := 0; i < blockCount; i++ {
		start := i * int(blockSize)
		end := start + int(blockSize)
		index := int64(i)*interval + offset

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
		if !assert.Equalf(sourceData[start:end], block, "block: %v", index) {
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
		pair, err := fetcher.FetchBlock()
		if assert.NoError(err) {
			start := i * int(blockSize)
			end := start + int(blockSize)
			index := indices[i]

			assert.Equalf(index, pair.Index, "block: %v", index)
			assert.Equalf(sourceData[start:end], pair.Block, "block: %v", index)
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
		pair, err := fetcher.FetchBlock()
		if assert.NoError(err) {
			start := i * int(blockSize)
			end := start + int(blockSize)
			index := indices[i]

			assert.Equalf(index, pair.Index, "block: %v", index)
			assert.Equalf(sourceData[start:end], pair.Block, "block: %v", index)
		}
	}

	_, err = fetcher.FetchBlock()
	assert.Equal(io.EOF, err)
}
