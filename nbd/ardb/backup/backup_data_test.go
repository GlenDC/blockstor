package backup

import "github.com/zero-os/0-Disk/config"

var validConfigs = []Config{
	// Explicit Example
	Config{
		VdiskID:             "foo",
		SnapshotID:          "foo",
		BlockSize:           DefaultBlockSize,
		BlockStorageConfig:  config.SourceConfig{},
		BackupStorageConfig: StorageConfig{},
		JobCount:            0,
		CompressionType:     LZ4Compression,
		CryptoKey:           CryptoKey{4, 2},
	},
	// implicit version of first example
	Config{
		VdiskID:   "foo",
		CryptoKey: CryptoKey{4, 2},
	},
	// full (FTP) example
	Config{
		VdiskID:    "foo",
		SnapshotID: "bar",
		BlockSize:  4096,
		BlockStorageConfig: config.SourceConfig{
			Resource:   "localhost:20021",
			SourceType: config.ETCDSourceType,
		},
		BackupStorageConfig: StorageConfig{
			Resource: FTPStorageConfig{
				Address:  "localhost:2000",
				Username: "root",
				Password: "secret",
			},
			StorageType: FTPStorageType,
		},
		JobCount:        1,
		CompressionType: XZCompression,
		CryptoKey: CryptoKey{
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
			0, 1},
	},
}

var invalidConfigs = []Config{
	// Nothing Given,
	Config{},
	// Invalid BlockSize
	Config{
		VdiskID:   "foo",
		BlockSize: 2000,
	},
	// Missing VdiskID
	Config{},
	// Bad Storage Config
	Config{
		VdiskID: "foo",
		BackupStorageConfig: StorageConfig{
			Resource: 42,
		},
	},
	// Bad Config Source
	Config{
		VdiskID: "foo",
		BlockStorageConfig: config.SourceConfig{
			SourceType: config.ETCDSourceType,
		},
	},
	// bad compression type
	Config{
		VdiskID:         "foo",
		CompressionType: CompressionType(42),
	},
}
