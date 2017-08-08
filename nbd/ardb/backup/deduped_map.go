package backup

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/pierrec/lz4"
	"github.com/zero-os/0-Disk/nbd/ardb/backup/schema"
	capnp "zombiezen.com/go/capnproto2"
)

func NewDedupedMap() *DedupedMap {
	return &DedupedMap{
		hashes:     make(map[int64][]byte),
		isFreshMap: true,
	}
}

type DedupedMap struct {
	hashes map[int64][]byte
	mux    sync.RWMutex

	isFreshMap bool
}

func (dm *DedupedMap) SetHash(index int64, hash []byte) bool {
	if !dm.isFreshMap {
		if h, found := dm.GetHash(index); found && bytes.Compare(h, hash) == 0 {
			return false // already have that hash for given index
		}
	}

	dm.mux.Lock()
	defer dm.mux.Unlock()

	dm.hashes[index] = hash
	return true // hash didn't exist yet for that index
}

func (dm *DedupedMap) GetHash(index int64) ([]byte, bool) {
	dm.mux.RLock()
	defer dm.mux.RUnlock()

	hash, found := dm.hashes[index]
	return hash, found
}

func (dm *DedupedMap) Bytes() ([]byte, error) {
	dm.mux.RLock()
	defer dm.mux.RUnlock()

	hashCount := len(dm.hashes)
	if hashCount == 0 {
		return nil, errors.New("deduped map is empty")
	}

	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, fmt.Errorf("failed to build msg+segment: %v", err)
	}
	dedupedMap, err := schema.NewRootDedupedMap(seg)
	if err != nil {
		return nil, fmt.Errorf("couldn't create capnp deduped map: %v", err)
	}
	hashes, err := dedupedMap.NewHashes(int32(hashCount))
	if err != nil {
		return nil, fmt.Errorf("couldn't create capnp hash list: %v", err)
	}

	for index, value := range dm.hashes {
		pair, err := schema.NewIndexHashPair(seg)
		if err != nil {
			return nil, fmt.Errorf("couldn't create capnp index-hash pair: %v", err)
		}
		pair.SetIndex(index)
		pair.SetHash(value)
		err = hashes.Set(int(index), pair)
		if err != nil {
			return nil, fmt.Errorf("couldn't set capnp index-hash pair %d: %v", index, err)
		}
	}

	var bufferA, bufferB bytes.Buffer
	err = capnp.NewEncoder(&bufferA).Encode(msg)
	if err != nil {
		return nil, fmt.Errorf("couldn't (capnp) serialize dedupd map: %v", err)
	}

	compressor := lz4.NewWriter(&bufferB)
	_, err = compressor.ReadFrom(&bufferA)
	if err == nil {
		err = compressor.Flush()
	}
	if err != nil {
		return nil, fmt.Errorf("couldn't (lz4) compress serialized dedupd map: %v", err)
	}

	// TODO! First ENCRYPT THEN ENCODE!
	// or first encode and then encrypt... zzzz

	bufferA.Reset()
	return nil, nil // TODO FINISH
}
