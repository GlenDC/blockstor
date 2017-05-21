package restore

import (
	"context"
	crand "crypto/rand"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"testing"
	"time"

	blockstorcfg "github.com/g8os/blockstor/config"
	"github.com/g8os/blockstor/gonbdserver/nbd"
	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog/tlogserver/server"

	"github.com/g8os/blockstor/redisstub"
	"github.com/g8os/blockstor/tlog"
	"github.com/stretchr/testify/assert"
)

func TestEndToEndReplayBootVdisk(t *testing.T) {
	testEndToEndReplay(t, blockstorcfg.VdiskTypeBoot)
}

func TestEndToEndReplayDBdisk(t *testing.T) {
	testEndToEndReplay(t, blockstorcfg.VdiskTypeDB)
}

func testEndToEndReplay(t *testing.T, vdiskType blockstorcfg.VdiskType) {
	// 1. Start a tlogserver;

	testConf := &server.Config{
		K:          4,
		M:          2,
		ListenAddr: "",
		FlushSize:  1,
		FlushTime:  1,
		PrivKey:    "12345678901234567890123456789012",
		HexNonce:   "37b8e8a308c354048d245f6d",
	}

	// create inmemory redis pool factory
	poolFactory := tlog.InMemoryRedisPoolFactory(testConf.RequiredDataServers())

	// start the server
	s, err := server.NewServer(testConf, poolFactory)
	if !assert.Nil(t, err) {
		return
	}

	go s.Listen()

	var (
		tlogrpc = s.ListenAddr()
	)

	t.Logf("listen addr=%v", tlogrpc)

	const (
		vdiskID       = "myvdisk"
		blockSize     = 4096
		size          = 1024 * 64
		firstSequence = 0
	)

	// 2. Start an NBDServer Backend with tlogclient integration;

	ctx := context.Background()

	backend, err := newTestBackend(ctx, t, vdiskID, vdiskType, tlogrpc, blockSize, size)
	if !assert.Nil(t, err) {
		return
	}

	// 3. Generate 64 KiB of random data (with some partial and full zero blocks)
	//    and write it to the nbd backend;

	data := make([]byte, size)
	_, err = crand.Read(data)
	if !assert.Nil(t, err) {
		return
	}
	blocks := size / blockSize

	zeroBlock := make([]byte, blockSize)

	for i := 0; i < blocks; i++ {
		offset := i * blockSize

		op := mrand.Int() % 10

		if op > 5 && op < 8 { // zero block
			_, err := backend.WriteZeroesAt(ctx, int64(offset), int64(blockSize))
			if !assert.Nil(t, err) {
				return
			}

			copy(data[offset:], zeroBlock)
			continue
		}

		if op > 8 {
			// partial zero block
			r := mrand.Int()
			size := r % (blockSize / 2)
			offset := offset + (r % (blockSize / 4))
			copy(data[offset:], zeroBlock[:size])
		}

		_, err := backend.WriteAt(ctx, data[offset:offset+blockSize], int64(offset))
		if !assert.Nil(t, err) {
			return
		}
	}

	// flush data
	err = backend.Flush(ctx)
	if !assert.Nil(t, err) {
		return
	}

	// give tlogserver time to flush
	time.Sleep(3 * time.Second)

	// 4. Validate that all the data is retrievable and correct;

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := backend.ReadAt(ctx, int64(offset), int64(blockSize))
		if !assert.Nil(t, err) {
			return
		}
		if !assert.Equal(t, data[offset:offset+blockSize], content) {
			return
		}
	}

	// 5. Wipe all data on the arbd (AKA create a new backend, hehe)
	//    this time without tlog integration though!!!!
	backend, err = newTestBackend(ctx, t, vdiskID, vdiskType, "", blockSize, size)
	if !assert.Nil(t, err) {
		return
	}

	// 6. Validate that the data is no longer retrievable via the backend;

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := backend.ReadAt(ctx, int64(offset), int64(blockSize))
		if !assert.Nil(t, err) {
			return
		}
		if !assert.Equal(t, zeroBlock, content) {
			return
		}
	}

	// 7. Replay the tlog aggregations;
	tlogRedisPool, err := poolFactory.NewRedisPool(vdiskID)
	if !assert.Nil(t, err) {
		return
	}

	err = decode(
		ctx, backend, tlogRedisPool, vdiskID,
		testConf.K, testConf.M, testConf.PrivKey, testConf.HexNonce)
	if !assert.Nil(t, err) {
		return
	}

	// give backend time to flush
	time.Sleep(3 * time.Second)

	// 8. Validate that all the data is again retrievable and correct;

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := backend.ReadAt(ctx, int64(offset), int64(blockSize))
		if !assert.Nil(t, err) {
			return
		}
		if !assert.Equal(t, data[offset:offset+blockSize], content) {
			return
		}
	}
}

// create a test backend
func newTestBackend(ctx context.Context, t *testing.T, vdiskID string, vdiskType blockstorcfg.VdiskType, tlogrpc string, blockSize, size uint64) (nbd.Backend, error) {
	ardbStorage := redisstub.NewMemoryRedis()

	nbdConfigFile, err := ioutil.TempFile("", "blockstor")
	if err != nil {
		return nil, err
	}

	go func() {
		defer ardbStorage.Close()
		defer os.Remove(nbdConfigFile.Name())
		ardbStorage.Listen()
	}()

	// create nbd config
	nbdConfig := &blockstorcfg.Config{
		Vdisks: map[string]blockstorcfg.VdiskConfig{
			vdiskID: blockstorcfg.VdiskConfig{
				BlockSize:      blockSize,
				ReadOnly:       false,
				Size:           size,
				StorageCluster: "mycluster",
				Type:           vdiskType,
			},
		},
		StorageClusters: map[string]blockstorcfg.StorageClusterConfig{
			"mycluster": blockstorcfg.StorageClusterConfig{
				DataStorage: []blockstorcfg.StorageServerConfig{
					blockstorcfg.StorageServerConfig{Address: ardbStorage.Address()},
				},
				MetadataStorage: blockstorcfg.StorageServerConfig{Address: ardbStorage.Address()},
			},
		},
	}

	// store nbd config in temporary location
	_, err = nbdConfigFile.Write([]byte(nbdConfig.String()))
	if err != nil {
		return nil, err
	}

	// create backend (finally)
	backend, err := newBackend(ctx, nil, tlogrpc, vdiskID, nbdConfigFile.Name())
	if err != nil {
		return nil, err
	}

	// start background thread
	go backend.GoBackground(ctx)

	// return backend
	return backend, nil
}

func init() {
	log.SetLevel(log.DebugLevel)
}
