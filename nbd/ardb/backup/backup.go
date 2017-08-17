package backup

import (
	"errors"
	"fmt"
	"io"
	"runtime"

	"github.com/zero-os/0-Disk/nbd/ardb"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

const (
	// DefaultBlockSize is the default block size,
	// used for the deduped blocks stored as a backup.
	DefaultBlockSize = 1024 * 128 // 128 KiB
)

// Config used to export/import a backup.
type Config struct {
	// Required: VdiskID to export from or import into
	VdiskID string
	// Optional: ID of the snapshot (same as VdiskID by default)
	SnapshotID string

	// Optional: Snapshot BlockSize (128KiB by default)
	BlockSize int64

	// Required: SourceConfig to configure the storage with
	StorageSource config.SourceConfig
	// Required: FTPServerConfig used to configure the destination FTP Server
	FTPServer FTPServerConfig

	// Optional: Amount of jobs (goroutines) to run simultaneously
	//           (to import/export in parallel)
	//           By default it equals the amount of CPUs available.
	JobCount int

	// Type of Compression to use for compressing/decompressing.
	// Note: this should be the same value for an import/export pair
	CompressionType CompressionType
	// CryptoKey to use for encryption/decryption.
	// Note: this should be the same value for an import/export pair
	CryptoKey CryptoKey
}

// validate the export/import config,
// and fill-in all the missing optional data.
func (cfg *Config) validate() error {
	if cfg.VdiskID == "" {
		return errNilVdiskID
	}
	if cfg.SnapshotID == "" {
		cfg.SnapshotID = cfg.VdiskID
	}

	// turn this into config.ValidateBlockSize(x)
	if cfg.BlockSize == 0 {
		cfg.BlockSize = DefaultBlockSize
	} else {
		if !config.ValidateBlockSize(cfg.BlockSize) {
			return fmt.Errorf("blockSize '%d' is not valid", cfg.BlockSize)
		}
	}

	err := cfg.StorageSource.Validate()
	if err != nil {
		return err
	}

	err = cfg.FTPServer.Validate()
	if err != nil {
		return err
	}

	if cfg.JobCount <= 0 {
		cfg.JobCount = runtime.NumCPU()
	}

	err = cfg.CompressionType.Validate()
	if err != nil {
		return err
	}

	return nil
}

// storageConfig returned when creating a block storage,
// ready to export to/import from a backup.
type storageConfig struct {
	Indices      []int64
	NBD          config.NBDStorageConfig
	BlockStorage storage.BlockStorageConfig
}

// blockFetcher is a generic interface which defines the API
// to fetch a block (and its index) until we io.EOF is reached.
type blockFetcher interface {
	// FetchBlock fetches a new block (and its index) every call,
	// io.EOF is returned in case no blocks are available any longer.
	FetchBlock() (*blockIndexPair, error)
}

// blockIndexPair is the result type for the `blockFetcher` API.
type blockIndexPair struct {
	// Block which has been fetched.
	Block []byte
	// Index of the Block which has been fetched.
	Index int64
}

// sizedBlockFetcher wraps the given blockFetcher,
// in case the dst- and src- blocksize don't match up.
// This way you can be sure that you're block fetcher,
// always returns blocks that match the expected destination size.
func sizedBlockFetcher(bf blockFetcher, srcBS, dstBS int64) blockFetcher {
	if srcBS < dstBS {
		return newInflationBlockFetcher(bf, srcBS, dstBS)
	}

	if srcBS > dstBS {
		return newDeflationBlockFetcher(bf, srcBS, dstBS)
	}

	// srcBS == dstBS
	return bf
}

// newInflationBlockFetcher creates a new Inflation BlockFetcher,
// wrapping around the given block fetcher.
// See `inflationBlockFetcher` for more information.
func newInflationBlockFetcher(bf blockFetcher, srcBS, dstBS int64) *inflationBlockFetcher {
	return &inflationBlockFetcher{
		in:    bf,
		srcBS: srcBS,
		dstBS: dstBS,
		ratio: dstBS / srcBS,
	}
}

// inflationBlockFetcher allows you to fetch bigger blocks,
// from an internal blockFetcher which itself returns smaller blocks.
type inflationBlockFetcher struct {
	in           blockFetcher
	srcBS, dstBS int64
	ratio        int64
}

// FetchBlock implements blockFetcher.FetchBlock
func (ibf *inflationBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	var err error
	var indexDelta int64

	output := make([]byte, ibf.dstBS)

	blockPair, err := ibf.in.FetchBlock()
	if err != nil {
		return nil, err
	}

	// ensure that we start at the correct local offset
	offset := (blockPair.Index % ibf.ratio) * ibf.srcBS
	// store the prevIndex, so we can use it for the next cycles
	prevIndex := blockPair.Index

	// copy the fetched block into our final destination block
	copy(output[offset:offset+ibf.srcBS], blockPair.Block)
	offset += ibf.srcBS

	// try to fill up the (bigger) destination block as much as possible
	for offset < ibf.dstBS {
		// we have still space for an actual block, let's fetch it
		blockPair, err = ibf.in.FetchBlock()
		if err != nil {
			if err == io.EOF {
				break // this is OK, as we'll just concider the rest of dst block as 0
			}

			return nil, err
		}

		// if our delta is bigger than 1,
		// we need to first move our offset, as to respect the original block spacing.
		indexDelta = blockPair.Index - prevIndex
		if prevIndex >= 0 && indexDelta > 1 {
			offset += (indexDelta - 1) * ibf.srcBS
			// if the offset goes now beyond the destination block size,
			// we can return the output, as we're done here
			if offset >= ibf.dstBS {
				return &blockIndexPair{
					Block: output,
					Index: prevIndex / ibf.ratio,
				}, nil
			}
		}

		// remember the prev index for the next cycle (if there is one)
		prevIndex = blockPair.Index

		// copy the fetched block into our final destination block
		copy(output[offset:offset+ibf.srcBS], blockPair.Block)
		offset += ibf.srcBS
	}

	// return a filled destination block
	return &blockIndexPair{
		Block: output,
		Index: prevIndex / ibf.ratio,
	}, nil
}

// newDeflationBlockFetcher creates a new Deflation BlockFetcher,
// wrapping around the given block fetcher.
// See `inflationBlockFetcher` for more information.
func newDeflationBlockFetcher(bf blockFetcher, srcBS, dstBS int64) *deflationBlockFetcher {
	return &deflationBlockFetcher{
		in:    bf,
		srcBS: srcBS,
		dstBS: dstBS,
		ratio: srcBS / dstBS,
		cb:    nil,
		cbi:   -1,
	}
}

// deflationBlockFetcher allows you to fetch smaller blocks,
// from an internal blockFetcher which itself returns bigger blocks.
type deflationBlockFetcher struct {
	in           blockFetcher
	srcBS, dstBS int64
	ratio        int64

	// current block
	cb  []byte // data
	cbi int64  // index
}

// FetchBlock implements blockFetcher.FetchBlock
func (dbf *deflationBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	var block []byte

	// continue fetching until we have a non-nil block
	for {
		for len(dbf.cb) > 0 {
			// continue distributing the already fetched block
			block = dbf.cb[:dbf.dstBS]
			// progress the cached slice
			dbf.cb = dbf.cb[dbf.dstBS:]

			if !isNilBlock(block) {
				// return the sliced block
				pair := &blockIndexPair{
					Block: block,
					Index: dbf.cbi,
				}
				dbf.cbi++
				return pair, nil
			}

			dbf.cbi++
		}

		// get next block, and recurse call this function,
		// such that we return the first part
		pair, err := dbf.in.FetchBlock()
		if err != nil {
			return nil, err
		}

		dbf.cb = pair.Block
		dbf.cbi = pair.Index * dbf.ratio
	}
}

// onceBlockFetcher is a fetcher which returns a pair just once,
// after which it will return io.EOF, until a new pair is given.
type onceBlockFetcher struct {
	pair *blockIndexPair
}

// FetchBlock implements blockFetcher.FetchBlock
func (obf *onceBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	if obf.pair == nil {
		return nil, io.EOF
	}

	pair := obf.pair
	obf.pair = nil
	return pair, nil
}

// isNilBlock returns true if the given block contains only 0.
func isNilBlock(block []byte) bool {
	for _, b := range block {
		if b != 0 {
			return false
		}
	}

	return true
}

// Create a block storage ready for importing/exporting to/from a backup.
func createBlockStorage(vdiskID string, sourceConfig config.SourceConfig, listIndices bool) (*storageConfig, error) {
	storageConfigCloser, err := config.NewSource(sourceConfig)
	if err != nil {
		return nil, err
	}
	defer storageConfigCloser.Close()

	vdiskConfig, err := config.ReadVdiskStaticConfig(storageConfigCloser, vdiskID)
	if err != nil {
		return nil, err
	}

	nbdStorageConfig, err := config.ReadNBDStorageConfig(storageConfigCloser, vdiskID, vdiskConfig)
	if err != nil {
		return nil, err
	}

	var indices []int64
	if listIndices {
		indices, err = storage.ListBlockIndices(vdiskID, vdiskConfig.Type, &nbdStorageConfig.StorageCluster)
		if err != nil {
			return nil, err
		}
	}

	blockStorage := storage.BlockStorageConfig{
		VdiskID:         vdiskID,
		TemplateVdiskID: vdiskConfig.TemplateVdiskID,
		VdiskType:       vdiskConfig.Type,
		BlockSize:       int64(vdiskConfig.BlockSize),
		LBACacheLimit:   ardb.DefaultLBACacheLimit,
	}

	return &storageConfig{
		Indices:      indices,
		NBD:          *nbdStorageConfig,
		BlockStorage: blockStorage,
	}, nil
}

var (
	errNilVdiskID       = errors.New("vdisk's identifier not given")
	errInvalidCryptoKey = errors.New("invalid crypto key")
)
