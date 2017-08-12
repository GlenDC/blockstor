package backup

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
)

func TestDedupedMapSerialization(t *testing.T) {
	dm := NewDedupedMap()

	const (
		hashCount = 1024 * 64
	)

	// set all hashes
	hashes := make([]zerodisk.Hash, hashCount)
	for i := 0; i < hashCount; i++ {
		hashes[i] = zerodisk.NewHash()
		_, err := rand.Read(hashes[i][:])
		if err != nil {
			t.Fatal(err)
		}

		dm.SetHash(int64(i), hashes[i])
	}

	// ensure all hashes were written
	for i := 0; i < hashCount; i++ {
		hash, ok := dm.GetHash(int64(i))
		if assert.True(t, ok) {
			assert.Equal(t, hashes[i], hash)
		}
	}

	var buf bytes.Buffer

	// serialize map
	err := dm.Serialize(&privKey, &buf)
	if !assert.NoError(t, err) {
		return
	}

	t.Logf(
		"deduped map of %d hashes (%d bytes) is serialized into %d bytes",
		hashCount, ((zerodisk.HashSize + 4) * hashCount), len(buf.Bytes()))

	// Deserialize map again
	dm, err = DeserializeDedupedMap(&privKey, &buf)
	if !assert.NoError(t, err) {
		return
	}

	// ensure all hashes are available
	for i := 0; i < hashCount; i++ {
		hash, ok := dm.GetHash(int64(i))
		if assert.True(t, ok) {
			assert.Equal(t, hashes[i], hash)
		}
	}
}
