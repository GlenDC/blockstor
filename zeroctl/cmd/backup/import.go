package backup

import (
	"context"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"

	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

// see `init` for more information
// about the meaning of each config property.
var importVdiskCmdCfg struct {
	Force bool
}

// ImportVdiskCmd represents the vdisk import subcommand
var ImportVdiskCmd = &cobra.Command{
	Use:   "vdisk vdiskid cryptoKey",
	Short: "import a vdisk",
	RunE:  importVdisk,
}

func importVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	// parse the position arguments
	err := parsePosArguments(args)
	if err != nil {
		return err
	}

	err = checkVdiskExists(vdiskCmdCfg.VdiskID)
	if err != nil {
		return err
	}

	// set snapshot id if it wasn't defined yet
	snapshotID := snapshotID(vdiskCmdCfg.SnapshotID, vdiskCmdCfg.VdiskID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := backup.Config{
		VdiskID:             vdiskCmdCfg.VdiskID,
		SnapshotID:          snapshotID,
		BlockSize:           vdiskCmdCfg.ExportBlockSize,
		BlockStorageConfig:  vdiskCmdCfg.SourceConfig,
		BackupStorageConfig: vdiskCmdCfg.BackupStorageConfig,
		JobCount:            vdiskCmdCfg.JobCount,
		CompressionType:     vdiskCmdCfg.CompressionType,
		CryptoKey:           vdiskCmdCfg.PrivateKey,
	}

	return backup.Import(ctx, cfg)
}

// checkVdiskExists checks if the vdisk in question already/still exists,
// and if so, and the force flag is specified, delete the (meta)data.
func checkVdiskExists(vdiskID string) error {
	// create config source
	configSource, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer configSource.Close()

	staticConfig, err := config.ReadVdiskStaticConfig(configSource, vdiskID)
	if err != nil {
		return fmt.Errorf(
			"cannot read static vdisk config for vdisk %s: %v", vdiskID, err)
	}
	nbdStorageConfig, err := config.ReadNBDStorageConfig(configSource, vdiskID, staticConfig)
	if err != nil {
		return fmt.Errorf(
			"cannot read nbd storage config for vdisk %s: %v", vdiskID, err)
	}

	exists, err := storage.VdiskExists(
		vdiskID, staticConfig.Type, &nbdStorageConfig.StorageCluster)
	if !exists {
		return nil // vdisk doesn't exist, so nothing to do
	}
	if err != nil {
		return fmt.Errorf("couldn't check if vdisk %s already exists: %v", vdiskID, err)
	}

	if !importVdiskCmdCfg.Force {
		return fmt.Errorf("cannot import vdisk %s as it already exists", vdiskID)
	}

	vdisks := map[string]config.VdiskType{vdiskID: staticConfig.Type}

	// delete metadata (if needed)
	if nbdStorageConfig.StorageCluster.MetadataStorage != nil {
		cfg := nbdStorageConfig.StorageCluster.MetadataStorage
		err := storage.DeleteMetadata(*cfg, vdisks)
		if err != nil {
			return fmt.Errorf(
				"couldn't delete metadata for vdisk %s from %s@%d: %v",
				vdiskID, cfg.Address, cfg.Database, err)
		}
	}

	// delete data (if needed)
	for _, serverConfig := range nbdStorageConfig.StorageCluster.DataStorage {
		err := storage.DeleteData(serverConfig, vdisks)
		if err != nil {
			return fmt.Errorf(
				"couldn't delete data for vdisk %s from %s@%d: %v",
				vdiskID, serverConfig.Address, serverConfig.Database, err)
		}
	}

	// vdisk did exist, but we were able to delete all the exiting (meta)data
	return nil
}

func init() {
	ImportVdiskCmd.Long = ImportVdiskCmd.Short + `

Remember to use the same snapshot identifier,
crypto (private) key and the compression type,
as you used while exporting the backup in question.

If an error occured during the import process,
blocks might already have been written to the block storage.
These blocks won't be deleted in case of an error,
so note that you might end up with some "garbage" in such a scenario.
Deleting the vdisk in such a scenario will help with this problem.

The FTP information is given as the -o, --output flag,
here are some examples of valid values for that flag:
	\t+ localhost:22
	\t+ ftp://1.2.3.4:200
	\t+ ftp://user@127.0.0.1:200
	\t+ ftp://user:pass@12.30.120.200:3000
`

	ImportVdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")

	ImportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.SnapshotID, "name", "",
		"the name of the backup (default `<vdiskID>_epoch`)")
	ImportVdiskCmd.Flags().Int64VarP(
		&vdiskCmdCfg.ExportBlockSize, "blocksize", "b", backup.DefaultBlockSize,
		"the size of the exported (deduped) blocks")
	ImportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.CompressionType, "compression", "c",
		"the compression type to use")
	ImportVdiskCmd.Flags().IntVarP(
		&vdiskCmdCfg.JobCount, "jobs", "j", runtime.NumCPU(),
		"the amount of parallel jobs to run")

	ImportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.BackupStorageConfig, "input", "i",
		"ftp server url or local dir path")

	ImportVdiskCmd.Flags().BoolVarP(
		&importVdiskCmdCfg.Force,
		"force", "f", false,
		"when given, delete the vdisk if it already existed")
}
