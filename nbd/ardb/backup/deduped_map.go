package backup

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/nbd/ardb/backup/schema"
	capnp "zombiezen.com/go/capnproto2"
)

func NewDedupedMap() *DedupedMap {
	return &DedupedMap{
		hashes: make(map[int64]zerodisk.Hash),
	}
}

func DeserializeDedupedMap(key *CryptoKey, src io.Reader) (*DedupedMap, error) {
	var bufA, bufB bytes.Buffer

	err := Decrypt(key, src, &bufA)
	if err != nil {
		return nil, fmt.Errorf("couldn't (AES256_GCM) decrypt compressed dedupd map: %v", err)
	}
	err = Decompress(&bufA, &bufB)
	if err != nil {
		return nil, fmt.Errorf("couldn't (LZ4) decompress dedupd map: %v", err)
	}

	msg, err := capnp.NewDecoder(&bufB).Decode()
	if err != nil {
		return nil, fmt.Errorf("couldn't decode (capnp) encoded deduped map: %v", err)
	}
	dedupedMap, err := schema.ReadRootDedupedMap(msg)
	if err != nil {
		return nil, fmt.Errorf("couldn't read (capnp) encoded deduped map: %v", err)
	}
	hashes, err := dedupedMap.Hashes()
	if err != nil {
		return nil, fmt.Errorf("couldn't read (capnp) hashes: %v", err)
	}

	n := hashes.Len()
	hashmap := make(map[int64]zerodisk.Hash, n)

	for i := 0; i < n; i++ {
		pair := hashes.At(i)
		hash, err := pair.Hash()
		if err != nil {
			return nil, fmt.Errorf("couldn't read hash #%d of %d hashes: %v", i, n, err)
		}

		hashmap[pair.Index()] = hash
	}

	return &DedupedMap{
		hashes: hashmap,
	}, nil
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

	hashCount := len(dm.hashes)
	if hashCount == 0 {
		return errors.New("deduped map is empty")
	}

	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return fmt.Errorf("failed to build msg+segment: %v", err)
	}
	dedupedMap, err := schema.NewRootDedupedMap(seg)
	if err != nil {
		return fmt.Errorf("couldn't create capnp deduped map: %v", err)
	}
	hashes, err := dedupedMap.NewHashes(int32(hashCount))
	if err != nil {
		return fmt.Errorf("couldn't create capnp hash list: %v", err)
	}

	for index, value := range dm.hashes {
		pair, err := schema.NewIndexHashPair(seg)
		if err != nil {
			return fmt.Errorf("couldn't create capnp index-hash pair: %v", err)
		}
		pair.SetIndex(index)
		pair.SetHash(value)
		err = hashes.Set(int(index), pair)
		if err != nil {
			return fmt.Errorf("couldn't set capnp index-hash pair %d: %v", index, err)
		}
	}

	var bufA, bufB bytes.Buffer
	err = capnp.NewEncoder(&bufA).Encode(msg)
	if err != nil {
		return fmt.Errorf("couldn't (capnp) serialize dedupd map: %v", err)
	}

	err = Compress(&bufA, &bufB)
	if err != nil {
		return fmt.Errorf("couldn't (lz4) compress serialized dedupd map: %v", err)
	}

	err = Encrypt(key, &bufB, dst)
	if err != nil {
		return fmt.Errorf("couldn't (AES256_GCM) encrypt compressed dedupd map: %v", err)
	}

	return nil
}
