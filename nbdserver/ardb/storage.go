package ardb

import (
	"context"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
)

// NewBlockStorage returns the correct block storage based on the given VdiskConfig.
// NOTE that this function will never return a tlogStorage,
// as that is a special storage type only ever to be used by the `ardb.Backend`.
func NewBlockStorage(vdiskID string, cfg config.VdiskConfig, provider ConnProvider) (storage BlockStorage, err error) {
	switch storageType := cfg.Type.StorageType(); storageType {
	case config.StorageDeduped:
		return NewDedupedStorage(
			vdiskID,
			int64(cfg.Size),
			int64(cfg.BlockSize),
			DefaultLBACacheLimit, // TODO: get this from config somehow?
			cfg.TemplateSupport(),
			provider)

	case config.StorageNonDeduped:
		return NewNonDedupedStorage(
			vdiskID,
			cfg.TemplateVdiskID,
			int64(cfg.BlockSize),
			cfg.TemplateSupport(),
			provider)

	case config.StorageSemiDeduped:
		return NewSemiDedupedStorage(
			vdiskID,
			int64(cfg.Size),
			int64(cfg.BlockSize),
			DefaultLBACacheLimit, // TODO: get this from config somehow?
			provider)

	default:
		return nil, fmt.Errorf(
			"no block storage available for %s's storage type %s",
			vdiskID, storageType)
	}
}

// BlockStorage defines an interface for all a block storage.
// It can be used to set, get and delete blocks.
//
// It is used by the `Backend` to implement the NBD Backend.
// Therefore this storage can be used by other modules,
// who need to manipulate the block storage for whatever reason.
type BlockStorage interface {
	Set(blockIndex int64, content []byte) (err error)
	Get(blockIndex int64) (content []byte, err error)
	Delete(blockIndex int64) (err error)

	Flush() (err error)

	Close() (err error)
	GoBackground(ctx context.Context)
}

// redisBytes is a utility function used by backendStorage functions,
// where we don't want to trigger an error for non-existent (or null) content.
func redisBytes(reply interface{}, replyErr error) (content []byte, err error) {
	content, err = redis.Bytes(reply, replyErr)
	// This could happen in case the block doesn't exist,
	// or in case the block is a nil block.
	// in both cases we want to simply return it as a nil block.
	if err == redis.ErrNil {
		err = nil
	}

	return
}
