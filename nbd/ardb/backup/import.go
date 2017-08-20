package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"sync"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

// Import a block storage from a FTP Server,
// decrypting and decompressing its blocks on the go.
func Import(ctx context.Context, cfg Config) error {
	err := cfg.validate()
	if err != nil {
		return err
	}

	storageConfig, err := createBlockStorage(cfg.VdiskID, cfg.BlockStorageConfig, false)
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

	importConfig := importConfig{
		JobCount:        cfg.JobCount,
		SrcBlockSize:    cfg.BlockSize,
		DstBlockSize:    storageConfig.BlockStorage.BlockSize,
		CompressionType: cfg.CompressionType,
		CryptoKey:       cfg.CryptoKey,
		SnapshotID:      cfg.SnapshotID,
	}

	return importBS(ctx, storageDriver, blockStorage, importConfig)
}

func importBS(ctx context.Context, src StorageDriver, dst storage.BlockStorage, cfg importConfig) error {
	// load the deduped map
	dedupedMap, err := LoadDedupedMap(cfg.SnapshotID, src, &cfg.CryptoKey, cfg.CompressionType)
	if err != nil {
		if err == ErrDataDidNotExist {
			return fmt.Errorf("no deduped map could be found using the id %s", cfg.SnapshotID)
		}

		return err
	}

	// setup the context that we'll use for all worker goroutines
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	inputCh := make(chan importInput, cfg.JobCount*2)   // gets closed by fetcher goroutine
	outputCh := make(chan importOutput, cfg.JobCount*2) // gets closed when all blocks have been fetched and processed

	errCh := make(chan error)
	defer close(errCh)

	sendErr := func(err error) {
		log.Errorf("an error occured while importing: %v", err)
		select {
		case errCh <- err:
		default:
		}
	}

	var importErr error
	// err ch used to
	go func() {
		select {
		case <-ctx.Done():
		case importErr = <-errCh:
			cancel() // stop all other goroutines
		}
	}()

	var wg sync.WaitGroup
	var owg sync.WaitGroup

	// launch all workers
	wg.Add(cfg.JobCount)
	for i := 0; i < cfg.JobCount; i++ {
		decompressor, err := NewDecompressor(cfg.CompressionType)
		if err != nil {
			return err
		}
		decrypter, err := NewDecrypter(&cfg.CryptoKey)
		if err != nil {
			return err
		}

		pipeline := &importPipeline{
			StorageDriver: src,
			Decrypter:     decrypter,
			Decompressor:  decompressor,
		}

		// launch worker
		go func(id int) {
			defer wg.Done()

			log.Debugf("starting import worker #%d", id)

			var input importInput
			var block []byte
			var open bool

			defer func() {
				if err != nil {
					log.Debugf("stopping import worker #%d with error: %v", id, err)
					return
				}
				log.Debugf("stopping export worker #%d", id)
			}()

			for {
				select {
				case <-ctx.Done():
					return

				case input, open = <-inputCh:
					if !open {
						return
					}

					// read, decrypt and decompress the input block hash
					block, err = pipeline.ReadBlock(input.BlockIndex, input.BlockHash)
					if err != nil {
						sendErr(err)
						return
					}

					// send block to storage goroutine (its final destination)
					output := importOutput{
						BlockData:     block,
						BlockIndex:    input.BlockIndex,
						SequenceIndex: input.SequenceIndex,
					}
					select {
					case <-ctx.Done():
						return
					case outputCh <- output:
					}
				}
			}
		}(i)
	}

	// launch output goroutine
	owg.Add(1)
	go func() {
		defer owg.Done()

		log.Debug("starting importer's output goroutine")

		var err error
		defer func() {
			if err != nil {
				log.Debugf("stopping importer's output goroutine with error: %v", err)
				return
			}
			log.Debug("stopping importer's output goroutine")
		}()

		sbf := newStreamBlockFetcher()
		obf := sizedBlockFetcher(sbf, cfg.SrcBlockSize, cfg.DstBlockSize)

		defer func() {
			if err != nil {
				return
			}

			sbf.streamStopped = true

			// if no error has yet occured,
			// ensure that at the end of this function,
			// the block fetcher is empty
			_, err = obf.FetchBlock()
			if err == nil || err != io.EOF {
				err = errors.New("output's block fetcher still has unstored content left")
				sendErr(err)
				return
			}
			err = nil
		}()

		var open bool
		var output importOutput
		var pair *blockIndexPair

		for {
			select {
			case <-ctx.Done():
				return
			case output, open = <-outputCh:
				if !open {
					if len(sbf.sequences) > 0 {
						err = fmt.Errorf(
							"output goroutine quits with %d invalid out-of-order blocks",
							len(sbf.sequences))
						sendErr(err)
					}
					return
				}

				if output.SequenceIndex < sbf.scursor {
					// NOTE: this should never happen,
					//       as it indicates a bug in the code
					err = fmt.Errorf(
						"unexpected sequence index returned, received %d, which is lower then %d",
						output.SequenceIndex, sbf.scursor)
					sendErr(err)
					return
				}

				// cache the current received output
				sbf.sequences[output.SequenceIndex] = blockIndexPair{
					Block: output.BlockData,
					Index: output.BlockIndex,
				}

				if output.SequenceIndex > sbf.scursor {
					// we received an out-of-order index,
					// so wait for the next one
					continue
				}

				// sequenceIndex == scursor
				// continue storing as much blocks as possible,
				// with the current cached output
				for {
					pair, err = obf.FetchBlock()
					if err != nil {
						if err == io.EOF || err == errStreamBlocked {
							err = nil
							break // we have nothing more to send (for now)
						}
						// unknown error, quit!
						sendErr(err)
						return
					}

					// store block,
					// which has been potentially sliced to fit the storage's block size
					err = dst.SetBlock(pair.Index, pair.Block)
					if err != nil {
						sendErr(err)
						return
					}
				}
			}
		}
	}()

	// launch fetcher, so it can start fetching hashes
	go func() {
		defer close(inputCh)

		log.Debug("starting importer's hash fetcher")

		var err error
		defer func() {
			if err != nil {
				log.Debugf("stopping importer's hash fetcher with error: %v", err)
				return
			}
			log.Debug("stopping importer's hash fetcher")
		}()

		hf := newHashFetcher(dedupedMap)
		var pair *indexHashPair

		var sequence int64

		// keep fetching hashes,
		// until we received an error,
		// where the error hopefully is just io.EOF
		for {
			select {
			case <-ctx.Done():
				return

			default:
				// fetch the next available block
				pair, err = hf.FetchHash()
				if err != nil {
					if err == io.EOF {
						err = nil
					} else {
						sendErr(err)
					}
					return
				}

				// attach a sequence to each block-index pair,
				// as the pipelines might process them out of order.
				input := importInput{
					BlockHash:     pair.Hash,
					BlockIndex:    pair.Index,
					SequenceIndex: sequence,
				}
				sequence++

				select {
				case <-ctx.Done():
					return
				case inputCh <- input:
				}
			}
		}
	}()

	// wait until all blocks have been fetched and processed
	wg.Wait()
	// close output ch, which will stop the output goroutine as soon as it's done
	close(outputCh)
	owg.Wait()

	// if an error occured, return it
	if importErr != nil {
		return importErr
	}

	// flush the block storage, and exit
	return dst.Flush()
}

// fetch -> decrypt -> decompress
type importPipeline struct {
	StorageDriver StorageDriver
	Decrypter     Decrypter
	Decompressor  Decompressor
}

func (p *importPipeline) ReadBlock(index int64, hash zerodisk.Hash) ([]byte, error) {
	bufA := bytes.NewBuffer(nil)
	err := p.StorageDriver.GetDedupedBlock(hash, bufA)
	if err != nil {
		return nil, err
	}

	blockHash := zerodisk.HashBytes(bufA.Bytes())
	if !hash.Equals(blockHash) {
		return nil, fmt.Errorf("block %d's hash does not match its content", index)
	}

	bufB := bytes.NewBuffer(nil)
	err = p.Decrypter.Decrypt(bufA, bufB)
	if err != nil {
		return nil, err
	}

	bufA.Reset()
	err = p.Decompressor.Decompress(bufB, bufA)
	if err != nil {
		return nil, err
	}

	bytes, err := ioutil.ReadAll(bufA)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func newHashFetcher(dedupedMap *DedupedMap) *hashFetcher {
	// collect and sort all index-hash pairs
	var pairs indexHashPairSlice
	for index, hash := range dedupedMap.hashes {
		pairs = append(pairs, indexHashPair{
			Index: index,
			Hash:  hash,
		})
	}
	sort.Sort(pairs)

	// return the hashFetcher ready for usage
	return &hashFetcher{
		pairs:  pairs,
		cursor: 0,
		length: int64(len(pairs)),
	}
}

// hashFetcher is used to fetch hashes,
// until all hashes have been read
type hashFetcher struct {
	pairs  indexHashPairSlice
	cursor int64
	length int64
}

func (hf *hashFetcher) FetchHash() (*indexHashPair, error) {
	// is the cursor OOB? If so, return EOF.
	if hf.cursor >= hf.length {
		return nil, io.EOF
	}

	// move the cursor ahead and return the previous hash
	hf.cursor++
	return &hf.pairs[hf.cursor-1], nil
}

// A pair of index and the hash it is mapped to.
type indexHashPair struct {
	Index int64
	Hash  zerodisk.Hash
}

// typedef used to be able to sort
type indexHashPairSlice []indexHashPair

type importConfig struct {
	JobCount int

	SrcBlockSize int64
	DstBlockSize int64

	CompressionType CompressionType
	CryptoKey       CryptoKey

	SnapshotID string
}

// implements Sort.Interface
func (ihps indexHashPairSlice) Len() int           { return len(ihps) }
func (ihps indexHashPairSlice) Less(i, j int) bool { return ihps[i].Index < ihps[j].Index }
func (ihps indexHashPairSlice) Swap(i, j int)      { ihps[i], ihps[j] = ihps[j], ihps[i] }

type importInput struct {
	BlockHash     zerodisk.Hash
	BlockIndex    int64
	SequenceIndex int64
}

type importOutput struct {
	BlockData     []byte // = data mapped to importInput.BlockHashHash
	BlockIndex    int64  // = importInput.BlockIndex
	SequenceIndex int64  // = importInput.SequenceIndex
}

func newStreamBlockFetcher() *streamBlockFetcher {
	return &streamBlockFetcher{
		sequences: make(map[int64]blockIndexPair),
	}
}

// streamBlockFetcher is a specialized blockFetcher,
// which is only supposed to be used for the import functionality,
// as it uses its sequence Index as the s(equence)cursor
type streamBlockFetcher struct {
	sequences     map[int64]blockIndexPair
	scursor       int64
	streamStopped bool
}

// FetchBlock implements blockFetcher.FetchBlock
func (sbf *streamBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	if sbf.streamStopped && len(sbf.sequences) == 0 {
		return nil, io.EOF
	}

	pair, ok := sbf.sequences[sbf.scursor]
	if !ok {
		return nil, errStreamBlocked
	}

	delete(sbf.sequences, sbf.scursor)
	sbf.scursor++
	return &pair, nil
}

var (
	errStreamBlocked     = errors.New("stream block fetcher is blocked waiting for next expected block")
	errInvalidBlockIndex = errors.New("block index could not be found in deduped map")
)
