package backup

import (
	"context"
	"crypto/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

func TestImportExportCycle_16_8_32(t *testing.T) {
	testImportExportCycle(t, 16, 8, 32)
}

func TestImportExportCycle_64_8_32(t *testing.T) {
	//testImportExportCycle(t, 64, 8, 32)
}

func TestImportExportCycle_8_8_32(t *testing.T) {
	testImportExportCycle(t, 8, 8, 32)
}

func TestImportExportCycle_64_64_32(t *testing.T) {
	testImportExportCycle(t, 64, 64, 32)
}

func TestImportExportCycle_8_16_32(t *testing.T) {
	testImportExportCycle(t, 8, 16, 32)
}

func TestImportExportCycle_8_64_32(t *testing.T) {
	testImportExportCycle(t, 8, 64, 32)
}

func testImportExportCycle(t *testing.T, srcBS, dstBS, blockCount int64) {
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
	srcMS := storage.NewInMemoryStorage(vdiskID, srcBS)
	// store all blocks in the source
	for index, block := range ibm {
		err = srcMS.SetBlock(index, block)
		if !assert.NoError(err) {
			return
		}
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
	dstMS := storage.NewInMemoryStorage(vdiskID, srcBS)

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

	var srcBlock, dstBlock []byte

	// ensure that both source and destination contain
	// the same blocks for the same indices
	for _, index := range indices {
		srcBlock, err = srcMS.GetBlock(index)
		if !assert.NoError(err) {
			continue
		}

		dstBlock, err = dstMS.GetBlock(index)
		if !assert.NoError(err) {
			continue
		}

		assert.Equal(srcBlock, dstBlock)
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
