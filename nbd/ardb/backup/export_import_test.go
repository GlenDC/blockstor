package backup

import (
	"context"
	"crypto/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/redisstub"
)

func TestImportExportCommute_8_2_16_MS(t *testing.T) {
	testImportExportCommute(t, 8, 2, 16, newInMemoryStorage)
}

func TestImportExportCommute_16_8_32_MS(t *testing.T) {
	testImportExportCommute(t, 16, 8, 32, newInMemoryStorage)
}

func TestImportExportCommute_64_8_32_MS(t *testing.T) {
	testImportExportCommute(t, 64, 8, 32, newInMemoryStorage)
}

func TestImportExportCommute_64_8_32_DS(t *testing.T) {
	testImportExportCommute(t, 64, 8, 32, newDedupedStorage)
}

func TestImportExportCommute_8_8_32_MS(t *testing.T) {
	testImportExportCommute(t, 8, 8, 32, newInMemoryStorage)
}

func TestImportExportCommute_64_64_32_MS(t *testing.T) {
	testImportExportCommute(t, 64, 64, 32, newInMemoryStorage)
}

func TestImportExportCommute_64_64_32_DS(t *testing.T) {
	testImportExportCommute(t, 64, 64, 32, newDedupedStorage)
}

func TestImportExportCommute_8_16_32_MS(t *testing.T) {
	testImportExportCommute(t, 8, 16, 32, newInMemoryStorage)
}

func TestImportExportCommute_8_64_32_MS(t *testing.T) {
	testImportExportCommute(t, 8, 64, 32, newInMemoryStorage)
}

func TestImportExportCommute_8_64_32_DS(t *testing.T) {
	testImportExportCommute(t, 8, 64, 32, newDedupedStorage)
}

type storageGenerator func(t *testing.T, vdiskID string, blockSize int64) (storage.BlockStorage, func())

func newInMemoryStorage(t *testing.T, vdiskID string, blockSize int64) (storage.BlockStorage, func()) {
	storage := storage.NewInMemoryStorage(vdiskID, blockSize)
	return storage, func() {
		storage.Close()
	}
}

func newDedupedStorage(t *testing.T, vdiskID string, blockSize int64) (storage.BlockStorage, func()) {
	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := storage.Deduped(vdiskID, blockSize, ardb.DefaultLBACacheLimit, false, redisProvider)
	if err != nil {
		t.Fatal(err)
	}
	return storage, func() {
		redisProvider.Close()
		storage.Close()
	}
}

func testImportExportCommute(t *testing.T, srcBS, dstBS, blockCount int64, sgen storageGenerator) {
	assert := assert.New(t)

	ibm, indices := generateImportExportData(srcBS, blockCount)

	const (
		vdiskID = "foo"
	)

	var err error

	// ctx used for this test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup source in-memory storage
	srcMS, srcMSClose := sgen(t, vdiskID, srcBS)
	defer srcMSClose()
	// store all blocks in the source
	for index, block := range ibm {
		err = srcMS.SetBlock(index, block)
		if !assert.NoError(err) {
			return
		}
	}
	err = srcMS.Flush()
	if !assert.NoError(err) {
		return
	}

	// setup stub driver to use for this test
	driver := newStubDriver()

	// export source in-memory storage
	exportCfg := exportConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    srcBS,
		DstBlockSize:    dstBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = exportBS(ctx, srcMS, indices, driver, exportCfg)
	if !assert.NoError(err) {
		return
	}

	// setup destination in-memory storage
	dstMS, dstMSClose := sgen(t, vdiskID, srcBS)
	defer dstMSClose()

	// import into destination in-memory storage
	importCfg := importConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    dstBS,
		DstBlockSize:    srcBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = importBS(ctx, driver, dstMS, importCfg)
	if !assert.NoError(err) {
		return
	}

	err = dstMS.Flush()
	if !assert.NoError(err) {
		return
	}

	var dstBlock []byte

	// ensure that both source and destination contain
	// the same blocks for the same indices
	for index, block := range ibm {
		dstBlock, err = dstMS.GetBlock(index)
		if !assert.NoError(err) {
			continue
		}

		assert.Equal(block, dstBlock)
	}
}

func generateImportExportData(blockSize, blockCount int64) (map[int64][]byte, []int64) {
	indexBlockMap := make(map[int64][]byte, blockCount)
	indices := make([]int64, blockCount)

	for i := int64(0); i < blockCount; i++ {
		data := make([]byte, blockSize)
		rand.Read(data)

		indexBlockMap[i] = data
		indices[i] = i
	}

	return indexBlockMap, indices
}

func init() {
	log.SetLevel(log.DebugLevel)
}
