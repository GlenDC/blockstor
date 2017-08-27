package storage

import (
	"github.com/zero-os/0-Disk"
)

// NewInMemoryStorage returns an in-memory BlockStorage implementation
func NewInMemoryStorage(vdiskID string, blockSize int64) BlockStorage {
	return &inMemoryStorage{
		blockSize: blockSize,
		vdiskID:   vdiskID,
	}
}

// inMemoryStorage is a BlockStorage implementation,
// that simply stores each block in-memory,
// only meant for dev and test purposes.
// Altought we might want to turn this into a proper supported storage,
// see the following open issue for more info:
// https://github.com/zero-os/0-Disk/issues/222
type inMemoryStorage struct {
	blockSize int64
	vdiskID   string
	vdisk     zerodisk.SyncMap
}

// SetBlock implements BlockStorage.SetBlock
func (ms *inMemoryStorage) SetBlock(blockIndex int64, content []byte) (err error) {
	// don't store zero blocks,
	// and delete existing ones if they already existed
	if ms.isZeroContent(content) {
		ms.vdisk.Delete(blockIndex)
		return
	}

	// content is not zero, so let's (over)write it
	ms.vdisk.Store(blockIndex, content)
	return
}

// GetBlock implements BlockStorage.GetBlock
func (ms *inMemoryStorage) GetBlock(blockIndex int64) (content []byte, err error) {
	value, ok := ms.vdisk.Load(blockIndex)
	if !ok {
		return
	}
	content, _ = value.([]byte)
	return
}

// DeleteBlock implements BlockStorage.DeleteBlock
func (ms *inMemoryStorage) DeleteBlock(blockIndex int64) (err error) {
	ms.vdisk.Delete(blockIndex)
	return
}

// Flush implements BlockStorage.Flush
func (ms *inMemoryStorage) Flush() (err error) {
	// nothing to do for the in-memory BlockStorage
	return
}

// isZeroContent detects if a given content buffer is completely filled with 0s
func (ms *inMemoryStorage) isZeroContent(content []byte) bool {
	for _, c := range content {
		if c != 0 {
			return false
		}
	}

	return true
}

// Close implements BlockStorage.Close
func (ms *inMemoryStorage) Close() error { return nil }
