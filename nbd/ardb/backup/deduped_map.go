package backup

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"

	"github.com/zeebo/bencode"
)

// NewDedupedMap creates a new deduped map,
// which contains all the metadata stored for a(n) (exported) backup.
// See `DedupedMap` for more information.
func NewDedupedMap() *DedupedMap {
	return &DedupedMap{
		hashes: new(zerodisk.SyncMap),
	}
}

// LoadDedupedMap a deduped map from a given (backup) server.
func LoadDedupedMap(id string, src StorageDriver, key *CryptoKey, ct CompressionType) (*DedupedMap, error) {
	buf := bytes.NewBuffer(nil)
	err := src.GetDedupedMap(id, buf)
	if err != nil {
		// deduped map did exist,
		// but an unknown error was triggered while fetching it
		return nil, err
	}

	// try to load the existing deduped map in memory
	return DeserializeDedupedMap(key, ct, buf)
}

// ExistingOrNewDedupedMap tries to first fetch an existing deduped map from a given server,
// if it doesn't exist yet, a new one will be created in-memory instead.
// If it did exist already, it will be decrypted, decompressed and loaded in-memory as a DedupedMap.
// When `force` is `true`, a new map will be created, even if one existed already but couldn't be loaded.
// When `force` is `false`, and a map exists but can't be a loaded,
// the error of why it couldn't be loaded, is returned instead.
func ExistingOrNewDedupedMap(id string, src StorageDriver, key *CryptoKey, ct CompressionType, force bool) (*DedupedMap, error) {
	buf := bytes.NewBuffer(nil)
	err := src.GetDedupedMap(id, buf)

	if err == ErrDataDidNotExist {
		// deduped map did not exist yet, return a new one
		return NewDedupedMap(), nil
	}
	if err != nil {
		// deduped map did exist, but we couldn't load it.
		if force {
			// we forcefully create a new one anyhow if `force == true`
			log.Debugf(
				"couldn't read deduped map '%s' due to an error (%s), forcefully creating a new one",
				id, err)
			return NewDedupedMap(), nil
		}
		// deduped map did exist,
		// but an unknown error was triggered while fetching it
		return nil, err
	}

	// try to load the existing deduped map in memory
	dm, err := DeserializeDedupedMap(key, ct, buf)
	if err != nil {
		// deduped map did exist, but we couldn't deserialize it.
		// This could for example happen in case the given encryption key is false,
		// or the compression algorithm doesn't match with the one used during serialization.

		// However, when `force` is given, we'll ignore this err and return a new deduped map instead.
		if force {
			log.Debugf(
				"couldn't deserialize deduped map '%s' due to an error (%s), forcefully creating a new one",
				id, err)
			return NewDedupedMap(), nil
		}

		return nil, err
	}

	log.Debugf("loaded and deserialized existing deduped map %s", id)
	return dm, err
}

// DeserializeDedupedMap allows you to deserialize a deduped map from a given reader.
// It is expected that all the data in the reader is available,
// and is compressed and (only than) encrypted.
// This function will attempt to decrypt and decompress the read data,
// using the given private (AES) key and compression type.
// The given compression type and private key has to match the information,
// used to serialize this DedupedMap in the first place.
// See `DedupedMap` for more information.
func DeserializeDedupedMap(key *CryptoKey, ct CompressionType, src io.Reader) (*DedupedMap, error) {
	decompressor, err := NewDecompressor(ct)
	if err != nil {
		return nil, err
	}

	bufA := bytes.NewBuffer(nil)
	bufB := bytes.NewBuffer(nil)

	err = Decrypt(key, src, bufA)
	if err != nil {
		return nil, fmt.Errorf("couldn't decrypt compressed deduped map: %v", err)
	}
	err = decompressor.Decompress(bufA, bufB)
	if err != nil {
		return nil, fmt.Errorf("couldn't decompress deduped map: %v", err)
	}

	hashes, err := deserializeHashes(bufB)
	if err != nil {
		return nil, fmt.Errorf("couldn't decode bencoded deduped map: %v", err)
	}

	return &DedupedMap{hashes: hashes}, nil
}

// DedupedMap contains all hashes for a vdisk's backup,
// where each hash is mapped to its (export) block index.
// NOTE: DedupedMap is not thread-safe,
//       and should only be used on one goroutine at a time.
type DedupedMap struct {
	hashes *zerodisk.SyncMap
}

// SetHash sets the given hash, mapped to the given (export block) index.
// If there is already a hash mapped to the given (export block) index,
// and the hash equals the given hash, the given hash won't be used and `false` wil be returned.
// Otherwise the given hash is mapped to the given index and `true`` will be returned.
func (dm *DedupedMap) SetHash(index int64, hash zerodisk.Hash) bool {
	_, loaded := dm.hashes.LoadOrStore(index, hash)
	return !loaded
}

// GetHash returns the hash which is mapped to the given (export block) index.
// `false` is returned in case no hash is mapped to the given (export block) index.
func (dm *DedupedMap) GetHash(index int64) (zerodisk.Hash, bool) {
	value, ok := dm.hashes.Load(index)
	if !ok {
		return nil, false
	}

	hash, ok := value.(zerodisk.Hash)
	return hash, ok
}

// Serialize allows you to write all data of this map in a binary encoded manner,
// to the given writer. The encoded data will be compressed and encrypted before being
// writen to the given writer.
// You can re-load this map in memory using the `DeserializeDedupedMap` function.
func (dm *DedupedMap) Serialize(key *CryptoKey, ct CompressionType, dst io.Writer) error {
	compressor, err := NewCompressor(ct)
	if err != nil {
		return err
	}

	hmbuffer := bytes.NewBuffer(nil)
	err = serializeHashes(dm.hashes, hmbuffer)
	if err != nil {
		return fmt.Errorf("couldn't bencode dedupd map: %v", err)
	}

	imbuffer := bytes.NewBuffer(nil)
	err = compressor.Compress(hmbuffer, imbuffer)
	if err != nil {
		return fmt.Errorf("couldn't compress bencoded dedupd map: %v", err)
	}

	err = Encrypt(key, imbuffer, dst)
	if err != nil {
		return fmt.Errorf("couldn't encrypt compressed dedupd map: %v", err)
	}

	return nil
}

// indexHashPairSlice returns this deduped map
// as an unsorted index-hash pair slice.
func (dm *DedupedMap) indexHashPairSlice() indexHashPairSlice {
	var slice indexHashPairSlice
	dm.hashes.Range(func(k, v interface{}) bool {
		slice = append(slice, indexHashPair{
			Index: k.(int64),
			Hash:  v.(zerodisk.Hash),
		})
		return true
	})
	return slice
}

// serializeHashes encapsulates the entire encoding logic
// for the deduped map serialization.
func serializeHashes(hashes *zerodisk.SyncMap, w io.Writer) error {
	var format dedupedMapEncodeFormat

	hashes.Range(func(k, v interface{}) bool {
		format.Indices = append(format.Indices, k.(int64))
		format.Hashes = append(format.Hashes, []byte(v.(zerodisk.Hash)))
		format.Count++
		return true
	})

	if format.Count == 0 {
		return errors.New("deduped map is empty")
	}

	return bencode.NewEncoder(w).Encode(format)
}

// deserializeHashes encapsulates the entire decoding logic
// for the deduped map serialization.
func deserializeHashes(r io.Reader) (*zerodisk.SyncMap, error) {
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

	var hashes zerodisk.SyncMap
	for i := int64(0); i < format.Count; i++ {
		hashes.Store(format.Indices[i], zerodisk.Hash(format.Hashes[i]))
	}

	return &hashes, nil
}

// dedupedMapEncodeFormat defines the structure used to
// encode a deduped map to a binary format.
// See https://github.com/zeebo/bencode for more information.
type dedupedMapEncodeFormat struct {
	Count   int64    `bencode:"c"`
	Indices []int64  `bencode:"i"`
	Hashes  [][]byte `bencode:"h"`
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
