package backup

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/zero-os/0-Disk"

	"github.com/marksamman/bencode"
)

func NewDedupedMap() *DedupedMap {
	return &DedupedMap{
		hashes: make(map[int64]zerodisk.Hash),
	}
}

func DeserializeDedupedMap(key *CryptoKey, src io.Reader) (*DedupedMap, error) {
	bufA := bytes.NewBuffer(nil)
	bufB := bytes.NewBuffer(nil)

	err := Decrypt(key, src, bufA)
	if err != nil {
		return nil, fmt.Errorf("couldn't (AES256_GCM) decrypt compressed deduped map: %v", err)
	}
	err = Decompress(bufA, bufB)
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

func (dm *DedupedMap) Serialize(key *CryptoKey, dst io.Writer) error {
	dm.mux.RLock()
	defer dm.mux.RUnlock()

	hmbuffer := bytes.NewBuffer(nil)
	err := serializeHashes(dm.hashes, hmbuffer)
	if err != nil {
		return fmt.Errorf("couldn't bencode dedupd map: %v", err)
	}

	imbuffer := bytes.NewBuffer(nil)
	err = Compress(hmbuffer, imbuffer)
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

	indices := make([]interface{}, hashCount)
	stringHashes := make([]interface{}, hashCount)
	var i int
	for index, hash := range hashes {
		indices[i] = index
		stringHashes[i] = string(hash.Bytes())
		i++
	}

	dict := make(map[string]interface{})
	dict[hashEncodeKeyCount] = hashCount
	dict[hashEncodeKeyIndices] = indices
	dict[hashEncodeKeyHashes] = stringHashes
	binaryEncoded := bencode.Encode(dict)

	_, err := w.Write(binaryEncoded)
	return err
}

func deserializeHashes(r io.Reader) (map[int64]zerodisk.Hash, error) {
	dict, err := bencode.Decode(r)
	if err != nil {
		return nil, fmt.Errorf("couldn't read deduped map: %v", err)
	}
	hashCount, ok := dict[hashEncodeKeyCount].(int64)
	if !ok {
		return nil, errors.New("couldn't read hash count")
	}
	indices, ok := dict[hashEncodeKeyIndices].([]interface{})
	if !ok || int64(len(indices)) != hashCount {
		return nil, errors.New("couldn't read indices")
	}
	stringHashes, ok := dict[hashEncodeKeyHashes].([]interface{})
	if !ok || int64(len(stringHashes)) != hashCount {
		return nil, errors.New("couldn't read hashes")
	}

	hashes := make(map[int64]zerodisk.Hash, hashCount)
	for i := int64(0); i < hashCount; i++ {
		strHash, ok := stringHashes[i].(string)
		if !ok || len(strHash) != zerodisk.HashSize {
			return nil, fmt.Errorf("invalid hash #%d", i)
		}
		hash := zerodisk.Hash(strHash)

		index, ok := indices[i].(int64)
		if !ok {
			return nil, fmt.Errorf("invalid index #%d", i)
		}

		hashes[index] = hash
	}

	return hashes, nil
}

const (
	hashEncodeKeyCount   = "c"
	hashEncodeKeyHashes  = "h"
	hashEncodeKeyIndices = "i"
)
