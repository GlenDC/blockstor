package backup

import (
	"bytes"
	"context"
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

	storageConfig, err := createBlockStorage(cfg.VdiskID, cfg.StorageSource, true)
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

	importConfig := importConfig{
		JobCount:        cfg.JobCount,
		SrcBlockSize:    cfg.BlockSize,
		DstBlockSize:    storageConfig.BlockStorage.BlockSize,
		CompressionType: cfg.CompressionType,
		CryptoKey:       cfg.CryptoKey,
		SnapshotID:      cfg.SnapshotID,
	}

	return importBS(ctx, ftpDriver, blockStorage, importConfig)
}

func importBS(ctx context.Context, src ServerDriver, dst storage.BlockStorage, cfg importConfig) error {
	// load the deduped map
	dedupedMap, err := LoadDedupedMap(cfg.SnapshotID, src, &cfg.CryptoKey, cfg.CompressionType)
	if err != nil {
		return err
	}

	// setup the context that we'll use for all worker goroutines
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	inputCh := make(chan indexHashPair, cfg.JobCount*2)   // gets closed by fetcher goroutine
	outputCh := make(chan blockIndexPair, cfg.JobCount*2) // gets closed when all blocks have been fetched and processed

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
			ServerDriver: src,
			Decrypter:    decrypter,
			Decompressor: decompressor,
		}

		// launch worker
		go func(id int) {
			defer wg.Done()

			log.Debugf("starting import worker #%d", id)

			var obf onceBlockFetcher
			// TODO: need to take into account that srcBS can be smaller than dstBS
			//       and thus the onceBlockFetcher might not really be useful in that case...
			dbf := newDeflationBlockFetcher(&obf, cfg.SrcBlockSize, cfg.DstBlockSize)

			var inputPair indexHashPair
			var outputPair *blockIndexPair
			var block []byte
			var open bool
			var err error

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

				case inputPair, open = <-inputCh:
					if !open {
						return
					}

					block, err = pipeline.ReadBlock(inputPair.Hash)
					if err != nil {
						sendErr(err)
						return
					}

					obf.pair = &blockIndexPair{
						Block: block,
						Index: inputPair.Index,
					}

				fetchLoop:
					for {
						select {
						case <-ctx.Done():
							return

						default:
							outputPair, err = dbf.FetchBlock()
							if err != nil {
								if err != io.EOF {
									sendErr(err)
									return
								}
								err = nil
								break fetchLoop // we're done
							}
							select {
							case <-ctx.Done():
								return
							case outputCh <- *outputPair:
							}
						}
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

		var open bool
		var pair blockIndexPair

		for {
			select {
			case <-ctx.Done():
				return
			case pair, open = <-outputCh:
				if !open {
					return
				}

				err = dst.SetBlock(pair.Index, pair.Block)
				if err != nil {
					sendErr(err)
					return
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

				select {
				case <-ctx.Done():
					return
				case inputCh <- *pair:
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
	ServerDriver ServerDriver
	Decrypter    Decrypter
	Decompressor Decompressor
}

func (p *importPipeline) ReadBlock(hash zerodisk.Hash) ([]byte, error) {
	bufA := bytes.NewBuffer(nil)
	err := p.ServerDriver.GetDedupedBlock(hash, bufA)
	if err != nil {
		return nil, err
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
