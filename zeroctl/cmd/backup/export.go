package backup

import (
	"context"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"

	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

// ExportVdiskCmd represents the vdisk export subcommand
var ExportVdiskCmd = &cobra.Command{
	Use:   "vdisk vdiskid cryptoKey",
	Short: "export a vdisk",
	RunE:  exportVdisk,
}

func exportVdisk(cmd *cobra.Command, args []string) error {
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

	err = backup.Export(ctx, cfg)
	if err != nil {
		return err
	}

	fmt.Println(snapshotID)
	return nil
}

func init() {
	ExportVdiskCmd.Long = ExportVdiskCmd.Short + `

Remember to keep note of the used snapshot identifier,
crypto (private) key and the compression type,
as you will need the same information when importing the exported backup.

If an error occured during the export process,
deduped blocks might already have been written to the FTP server.
These blocks won't be deleted in case of an error,
so note that you might end up with some "garbage" in such a scenario.

The FTP information is given using the -i, --input flaag,
here are some examples of valid values for that flag:
	\t+ localhost:22
	\t+ ftp://1.2.3.4:200
	\t+ ftp://user@127.0.0.1:200
	\t+ ftp://user:pass@12.30.120.200:3000
`

	ExportVdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")

	ExportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.SnapshotID, "name", "",
		"the name of the backup (default `<vdiskID>_epoch`)")
	ExportVdiskCmd.Flags().Int64VarP(
		&vdiskCmdCfg.ExportBlockSize, "blocksize", "b", backup.DefaultBlockSize,
		"the size of the exported (deduped) blocks")
	ExportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.CompressionType, "compression", "c",
		"the compression type to use")
	ExportVdiskCmd.Flags().IntVarP(
		&vdiskCmdCfg.JobCount, "jobs", "j", runtime.NumCPU(),
		"the amount of parallel jobs to run")

	ExportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.BackupStorageConfig, "output", "o",
		"ftp server url or local dir path")
}
