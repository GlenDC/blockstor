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
	Use:   "vdisk vdiskid cryptoKey ftpurl",
	Short: "export a vdisk",
	RunE:  exportVdisk,
}

func exportVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.InfoLevel
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
		VdiskID:         vdiskCmdCfg.VdiskID,
		SnapshotID:      snapshotID,
		BlockSize:       vdiskCmdCfg.ExportBlockSize,
		StorageSource:   vdiskCmdCfg.SourceConfig,
		FTPServer:       vdiskCmdCfg.FTPServerConfig,
		JobCount:        vdiskCmdCfg.JobCount,
		CompressionType: vdiskCmdCfg.CompressionType,
		CryptoKey:       vdiskCmdCfg.PrivateKey,
	}

	return backup.Export(ctx, cfg)
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

The FTP information is given as the third argument,
here are some examples of valid values for that argument:
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
		"the name of the backup (default: `<vdiskID>_epoch`)")
	ExportVdiskCmd.Flags().Int64VarP(
		&vdiskCmdCfg.ExportBlockSize, "blocksize", "b", backup.DefaultBlockSize,
		fmt.Sprintf(
			"the size of the exported (deduped) blocks (default: %d)",
			backup.DefaultBlockSize))
	ExportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.CompressionType, "compression", "c",
		fmt.Sprintf(
			"the compression type to use (default: %s)",
			vdiskCmdCfg.CompressionType.String()))
	ExportVdiskCmd.Flags().IntVarP(
		&vdiskCmdCfg.JobCount, "jobs", "j", runtime.NumCPU(),
		fmt.Sprintf(
			"the amount of parallel jobs to run (default: %d)",
			runtime.NumCPU()))
}
