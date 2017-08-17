package backup

import "github.com/zero-os/0-Disk/config"

var validConfigs = []Config{
	// Explicit Example
	Config{
		VdiskID:       "foo",
		SnapshotID:    "foo",
		BlockSize:     DefaultBlockSize,
		StorageSource: config.SourceConfig{},
		FTPServer: FTPServerConfig{
			Address:  "localhost:22",
			Username: "",
			Password: "",
		},
		JobCount:        0,
		CompressionType: LZ4Compression,
		CryptoKey:       CryptoKey{4, 2},
	},
	// implicit version of first example
	Config{
		VdiskID: "foo",
		FTPServer: FTPServerConfig{
			Address: "localhost:22",
		},
		CryptoKey: CryptoKey{4, 2},
	},
	// full example
	Config{
		VdiskID:    "foo",
		SnapshotID: "bar",
		BlockSize:  4096,
		StorageSource: config.SourceConfig{
			Resource:   "localhost:20021",
			SourceType: config.ETCDSourceType,
		},
		FTPServer: FTPServerConfig{
			Address:  "localhost:2000",
			Username: "root",
			Password: "secret",
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
		FTPServer: FTPServerConfig{
			Address: "localhost:2000",
		},
	},
	// Missing VdiskID
	Config{
		FTPServer: FTPServerConfig{
			Address: "localhost:2000",
		},
	},
	// Bad FTP Server Config
	Config{
		VdiskID: "foo",
	},
	// Bad Config Source
	Config{
		VdiskID: "foo",
		StorageSource: config.SourceConfig{
			SourceType: config.ETCDSourceType,
		},
		FTPServer: FTPServerConfig{
			Address: "localhost:2000",
		},
	},
	// bad compression type
	Config{
		VdiskID: "foo",
		FTPServer: FTPServerConfig{
			Address: "localhost:2000",
		},
		CompressionType: CompressionType(42),
	},
}
