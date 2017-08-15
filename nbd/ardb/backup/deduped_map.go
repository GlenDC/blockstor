package backup

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/zero-os/0-Disk"

	"github.com/zeebo/bencode"
)

func NewDedupedMap() *DedupedMap {
	return &DedupedMap{
		hashes: make(map[int64]zerodisk.Hash),
	}
}

func DeserializeDedupedMap(key *CryptoKey, ct CompressionType, src io.Reader) (*DedupedMap, error) {
	decompressor, err := NewDecompressor(ct)
	if err != nil {
		return nil, err
	}

	bufA := bytes.NewBuffer(nil)
	bufB := bytes.NewBuffer(nil)

	err = Decrypt(key, src, bufA)
	if err != nil {
		return nil, fmt.Errorf("couldn't (AES256_GCM) decrypt compressed deduped map: %v", err)
	}
	err = decompressor.Decompress(bufA, bufB)
	if err != nil {
		return nil, fmt.Errorf("couldn't (LZ4) decompress deduped map: %v", err)
	}

	hashes, err := deserializeHashes(bufB)
	if err != nil {
		return nil, fmt.Errorf("couldn't decode bencoded deduped map: %v", err)
	}

	return &DedupedMap{hashes: hashes}, nil
}

type DedupedMap struct {
	hashes map[int64]zerodisk.Hash
	mux    sync.RWMutex
}

func (dm *DedupedMap) SetHash(index int64, hash zerodisk.Hash) {
	dm.mux.Lock()
	defer dm.mux.Unlock()
	dm.hashes[index] = hash
}

func (dm *DedupedMap) GetHash(index int64) (zerodisk.Hash, bool) {
	dm.mux.RLock()
	defer dm.mux.RUnlock()

	hash, found := dm.hashes[index]
	return hash, found
}

func (dm *DedupedMap) Serialize(key *CryptoKey, ct CompressionType, dst io.Writer) error {
	compressor, err := NewCompressor(ct)
	if err != nil {
		return err
	}

	dm.mux.RLock()
	defer dm.mux.RUnlock()

	hmbuffer := bytes.NewBuffer(nil)
	err = serializeHashes(dm.hashes, hmbuffer)
	if err != nil {
		return fmt.Errorf("couldn't bencode dedupd map: %v", err)
	}

	imbuffer := bytes.NewBuffer(nil)
	err = compressor.Compress(hmbuffer, imbuffer)
	if err != nil {
		return fmt.Errorf("couldn't (lz4) compress bencoded dedupd map: %v", err)
	}

	err = Encrypt(key, imbuffer, dst)
	if err != nil {
		return fmt.Errorf("couldn't (AES256_GCM) encrypt compressed dedupd map: %v", err)
	}

	return nil
}

func serializeHashes(hashes map[int64]zerodisk.Hash, w io.Writer) error {
	hashCount := len(hashes)
	if hashCount == 0 {
		return errors.New("deduped map is empty")
	}

	var format dedupedMapEncodeFormat
	format.Count = int64(hashCount)

	format.Indices = make([]int64, hashCount)
	format.Hashes = make([][]byte, hashCount)

	var i int
	for index, hash := range hashes {
		format.Indices[i] = index
		format.Hashes[i] = hash.Bytes()
		i++
	}

	return bencode.NewEncoder(w).Encode(format)
}

func deserializeHashes(r io.Reader) (map[int64]zerodisk.Hash, error) {
	var format dedupedMapEncodeFormat
	err := bencode.NewDecoder(r).Decode(&format)
	if err != nil {
		return nil, fmt.Errorf("couldn't decode bencoded deduped map: %v", err)
	}

	if format.Count == 0 {
		return nil, errors.New("invalid count for decoded deduped map")
	}
	if format.Count != int64(len(format.Indices)) {
		return nil, errors.New("invalid index count for decoded deduped map")
	}
	if format.Count != int64(len(format.Hashes)) {
		return nil, errors.New("invalid hash count for decoded deduped map")
	}

	hashes := make(map[int64]zerodisk.Hash, format.Count)
	for i := int64(0); i < format.Count; i++ {
		hashes[format.Indices[i]] = zerodisk.Hash(format.Hashes[i])
	}

	return hashes, nil
}

type dedupedMapEncodeFormat struct {
	Count   int64    `bencode:"c"`
	Indices []int64  `bencode:"i"`
	Hashes  [][]byte `bencode:"h"`
}
