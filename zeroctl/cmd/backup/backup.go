package backup

import (
	"errors"
	"fmt"
	"time"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"
)

// see `init` and `parsePosArguments` for more information
// about the meaning of each config property.
var vdiskCmdCfg struct {
	VdiskID             string                 // required
	SourceConfig        config.SourceConfig    // optional
	SnapshotID          string                 // optional
	ExportBlockSize     int64                  // optional
	BackupStorageConfig backup.StorageConfig   // optional
	PrivateKey          backup.CryptoKey       // required
	CompressionType     backup.CompressionType // optional
	JobCount            int                    // optional
}

// snapshotID defaults to `<vdiskID>_epoch` if not defined
func snapshotID(id, vdiskID string) string {
	if id == "" {
		epoch := time.Now().UTC().Unix()
		return fmt.Sprintf("%s_%d", vdiskID, epoch)
	}

	return id
}

func parsePosArguments(args []string) error {
	// validate pos arg length
	argn := len(args)
	if argn < 2 {
		return errors.New("not enough arguments")
	} else if argn > 2 {
		return errors.New("too many arguments")
	}

	vdiskCmdCfg.VdiskID = args[0]

	return vdiskCmdCfg.PrivateKey.Set(args[1])
}
