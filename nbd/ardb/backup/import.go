package backup

import (
	"bytes"
	"io"
	"io/ioutil"
	"sort"

	"github.com/zero-os/0-Disk"
)

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

func newServerBlockFetcher(dedupedMap *DedupedMap, server ServerDriver) *serverBlockFetcher {
	// collect and sort all index-hash pairs
	var pairs indexHashPairSlice
	for index, hash := range dedupedMap.hashes {
		pairs = append(pairs, indexHashPair{
			Index: index,
			Hash:  hash,
		})
	}
	sort.Sort(pairs)

	// return the serverBlockFetcher ready for usage
	return &serverBlockFetcher{
		server: server,
		pairs:  pairs,
		cursor: 0,
		length: int64(len(pairs)),
	}
}

// serverBlockFetcher is used to fetch blocks from a (backup) server.
// All blocks are fetched in order of the (export) block index.
type serverBlockFetcher struct {
	server ServerDriver
	pairs  indexHashPairSlice
	cursor int64
	length int64
}

// FetchBlock implements blockFetcher.FetchBlock
func (sbf *serverBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	// is the cursor OOB? If so, return EOF.
	if sbf.cursor >= sbf.length {
		return nil, io.EOF
	}

	// get the current index-hash pair.
	current := sbf.pairs[sbf.cursor]

	// fetch the deduped block
	buf := bytes.NewBuffer(nil)
	err := sbf.server.GetDedupedBlock(current.Hash, buf)
	if err != nil {
		return nil, err
	}
	pair := &blockIndexPair{
		Index: current.Index,
		Block: buf.Bytes(),
	}

	// move the cursor ahead
	sbf.cursor++

	return pair, nil
}

// A pair of index and the hash it is mapped to.
type indexHashPair struct {
	Index int64
	Hash  zerodisk.Hash
}

// typedef used to be able to sort
type indexHashPairSlice []indexHashPair

// implements Sort.Interface
func (ihps indexHashPairSlice) Len() int           { return len(ihps) }
func (ihps indexHashPairSlice) Less(i, j int) bool { return ihps[i].Index < ihps[j].Index }
func (ihps indexHashPairSlice) Swap(i, j int)      { ihps[i], ihps[j] = ihps[j], ihps[i] }
