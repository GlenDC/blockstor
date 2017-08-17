package backup

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
)

func TestServerBlockFetcher(t *testing.T) {
	assert := assert.New(t)

	const (
		vdiskID    = "a"
		blockCount = 8
		blockSize  = 8
	)

	// create test data
	dedupedMap, stubServer, ibmapping := generateTestServerBlockFetcherData(blockCount, blockSize)
	if !assert.NotNil(dedupedMap) || !assert.NotNil(stubServer) || !assert.NotEmpty(ibmapping) {
		return
	}

	// test fetcher with all blocks

	fetcher := newServerBlockFetcher(dedupedMap, stubServer)
	if !assert.NotNil(fetcher) {
		return
	}

	for i := 0; i < blockCount; i++ {
		pair, err := fetcher.FetchBlock()
		if assert.NoError(err) {
			assert.Equalf(int64(i), pair.Index, "block: %v", i)
			assert.Equalf(ibmapping[int64(i)], pair.Block, "block: %v", i)
		}
	}

	_, err := fetcher.FetchBlock()
	assert.Equal(io.EOF, err)

	// test fetcher with half of available blocks

	fetcher.cursor = 0
	fetcher.pairs = fetcher.pairs[:blockCount/2]
	fetcher.length = int64(len(fetcher.pairs))

	for i := 0; i < blockCount/2; i++ {
		pair, err := fetcher.FetchBlock()
		if assert.NoError(err) {
			assert.Equalf(int64(i), pair.Index, "block: %v", i)
			assert.Equalf(ibmapping[int64(i)], pair.Block, "block: %v", i)
		}
	}

	_, err = fetcher.FetchBlock()
	assert.Equal(io.EOF, err)
}

func generateTestServerBlockFetcherData(n int64, blockSize int64) (*DedupedMap, *stubDriver, map[int64][]byte) {
	dm := NewDedupedMap()
	sd := newStubDriver()
	mapping := make(map[int64][]byte)

	for i := int64(0); i < n; i++ {
		data := make([]byte, blockSize)
		rand.Read(data)
		hash := zerodisk.HashBytes(data)
		dm.SetHash(i, hash)

		sd.dedupedBlocks[string(hash)] = data
		mapping[i] = data
	}

	return dm, sd, mapping
}
