package backup

import (
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"

	valid "github.com/asaskevich/govalidator"
	"github.com/secsy/goftp"
)

var (
	// ErrDataDidNotExist is returned from a ServerDriver's Getter
	// method in case the requested data does not exist on the server.
	ErrDataDidNotExist = errors.New("requested data did not exist")
)

// NewFTPStorageConfig creates a new FTP Storage Config,
// based on a given string
func NewFTPStorageConfig(data string) (cfg FTPStorageConfig, err error) {
	parts := ftpURLRegexp.FindStringSubmatch(data)
	if len(parts) != 6 {
		err = fmt.Errorf("'%s' is not a valid FTP URL", data)
		return
	}

	// set credentials
	cfg.Username = parts[1]
	cfg.Password = parts[2]

	// set address
	cfg.Address = parts[3] + parts[4]
	if parts[4] == "" {
		cfg.Address += ":22"
	}

	cfg.RootDir = parts[5]

	// all info was set, ensure to validate it while we're at it
	err = cfg.validate()
	return
}

// FTPStorageConfig is used to configure and create an FTP (Storage) Driver.
type FTPStorageConfig struct {
	// Address of the FTP Server
	Address string

	// Optional: username of Authorized account
	//           for the given FTP server
	Username string
	// Optional: password of Authorized account
	//           for the given FTP server
	Password string

	RootDir string // optional
}

// String implements Stringer.String
func (cfg *FTPStorageConfig) String() string {
	if cfg.validate() != nil {
		return "" // invalid config
	}

	url := "ftp://"
	if cfg.Username != "" {
		url += cfg.Username
		if cfg.Password != "" {
			url += ":" + cfg.Password
		}
		url += "@" + cfg.Address
	} else {
		url += cfg.Address
	}

	if cfg.RootDir != "" {
		url += cfg.RootDir
	}

	return url
}

// Set implements flag.Value.Set
func (cfg *FTPStorageConfig) Set(str string) error {
	var err error
	*cfg, err = NewFTPStorageConfig(str)
	return err
}

// Type implements PFlag.Type
func (cfg *FTPStorageConfig) Type() string {
	return "FTPStorageConfig"
}

// ftpURLRegexp is a simplistic ftp URL regexp,
// to be able to split an FTP url into credentials and the hostname.
// It doesn't contain much validation, as that isn't the point of this regexp.
var ftpURLRegexp = regexp.MustCompile(`^(?:ftp://)?(?:([^:@]+)(?::([^:@]+))?@)?(?:([^@/:]+)(:[0-9]+)?(/.*)?)$`)

// validate the FTP Storage Config.
func (cfg *FTPStorageConfig) validate() error {
	if cfg == nil {
		return nil
	}

	if cfg.Address == "" {
		return errors.New("no ftp server address given")
	}
	if !valid.IsURL(cfg.Address + cfg.RootDir) {
		return errors.New("invalid ftp server address given")
	}

	if cfg.Password != "" && cfg.Username == "" {
		return errors.New("password given, while username is missing")
	}

	return nil
}

// FTPStorageDriver ceates a driver which allows you
// to read/write deduped blocks/map from/to a FTP server.
func FTPStorageDriver(cfg FTPStorageConfig) (StorageDriver, error) {
	err := cfg.validate()
	if err != nil {
		return nil, err
	}

	config := goftp.Config{
		User:               cfg.Username,
		Password:           cfg.Password,
		ConnectionsPerHost: 10,
		Timeout:            10 * time.Second,
		Logger:             newFTPLogger(cfg.Address),
	}

	client, err := goftp.DialConfig(config, cfg.Address)
	if err != nil {
		return nil, err
	}

	return &ftpStorageDriver{
		client:    client,
		knownDirs: newDirCache(),
		rootDir:   strings.Trim(cfg.RootDir, "/"),
	}, nil
}

type ftpStorageDriver struct {
	client    *goftp.Client
	knownDirs *dirCache
	rootDir   string

	fileExistsMux sync.Mutex
	fileExists    func(string) bool
}

// SetDedupedBlock implements ServerDriver.SetDedupedBlock
func (ftp *ftpStorageDriver) SetDedupedBlock(hash zerodisk.Hash, r io.Reader) error {
	dir, file, ok := hashAsDirAndFile(hash)
	if !ok {
		return errInvalidHash
	}

	dir, err := ftp.mkdirs(dir)
	if err != nil {
		return err
	}

	return ftp.lazyStore(path.Join(dir, file), r)
}

// SetDedupedMap implements ServerDriver.SetDedupedMap
func (ftp *ftpStorageDriver) SetDedupedMap(id string, r io.Reader) error {
	dir, err := ftp.mkdirs(backupDir)
	if err != nil {
		return err
	}

	return ftp.lazyStore(path.Join(dir, id), r)
}

// GetDedupedBlock implements ServerDriver.GetDedupedBlock
func (ftp *ftpStorageDriver) GetDedupedBlock(hash zerodisk.Hash, w io.Writer) error {
	dir, file, ok := hashAsDirAndFile(hash)
	if !ok {
		return errInvalidHash
	}

	loc := path.Join(ftp.rootDir, dir, file)
	return ftp.retrieve(loc, w)
}

// GetDedupedMap implements ServerDriver.GetDedupedMap
func (ftp *ftpStorageDriver) GetDedupedMap(id string, w io.Writer) error {
	loc := path.Join(ftp.rootDir, backupDir, id)
	return ftp.retrieve(loc, w)
}

// Close implements ServerDriver.Close
func (ftp *ftpStorageDriver) Close() error {
	return ftp.client.Close()
}

// mkdirs walks through the given directory, one level at a time.
// If a level does not exists yet, it will be created.
// An error is returned if any of the used FTP commands returns an error,
// or in case a file at some point already exist, but is not a directory.
// The returned string is how the client should refer to the created directory.
func (ftp *ftpStorageDriver) mkdirs(dir string) (string, error) {
	dir = path.Join(ftp.rootDir, dir)

	// cheap early check
	// if we already known about the given dir,
	// than we know it exists (or at least we assume it does)
	if ftp.knownDirs.CheckDir(dir) {
		return dir, nil // early success return
	}

	// split the given dir path into directories, level per level
	dirs := strings.Split(dir, "/")

	// start at the driver's root
	var pwd string
	// walk through the entire directory structure, one level at a time
	for _, d := range dirs {
		// join the current level with our last known location
		pwd = path.Join(pwd, d)

		// check if already checked this dir earlier
		if ftp.knownDirs.CheckDir(pwd) {
			continue // current level exists (or we at least assume it does)
		}

		// create the current (sub) directory
		output, err := ftp.client.Mkdir(pwd)
		if err != nil {
			if !isFTPErrorCode(ftpErrorExists, err) {
				return "", err // return unexpected error
			}
		} else {
			pwd = output // only assign output in non-err case
		}

		ftp.knownDirs.AddDir(pwd)
	}

	// all directories either existed or were created
	// return the full path
	return pwd, nil
}

// lazyStore only stores a file if it doesn't exist yet already.
// An error is returned if any of the used FTP commands returns an error,
// or in case the given path already exists and is a directory, instead of a file.
func (ftp *ftpStorageDriver) lazyStore(path string, r io.Reader) error {
	// get info about the given path
	info, err := ftp.client.Stat(path)
	if err != nil {
		// if an error is received,
		// let's check if simply telling us the file doesn't exist yet,
		// if so, let's create it now.
		if isFTPErrorCode(ftpErrorNoExists, err) || isFTPErrorCode(ftpErrorInvalidCommand, err) {
			return ftp.client.Store(path, r)
		}
		return err
	}

	// path existed, and no error was returned.
	// Let's ensure the path points to a file, and not a directory.
	if info.IsDir() {
		return errors.New(path + " is a dir")
	}

	return nil // file already exists, nothing to do
}

// retrieve data from an FTP server.
// returns ErrDataDidNotExist in case there was no data available on the given path.
func (ftp *ftpStorageDriver) retrieve(path string, dest io.Writer) error {
	err := ftp.client.Retrieve(path, dest)
	if isFTPErrorCode(ftpErrorNoExists, err) {
		return ErrDataDidNotExist
	}
	return err
}

// list of ftp error codes we care about
const (
	ftpErrorInvalidCommand = 500
	ftpErrorNoExists       = 550
	ftpErrorExists         = 550
)

// small util function which allows us to check if an error
// is an FTP error, and if so, if it's the code we are looking for.
func isFTPErrorCode(code int, err error) bool {
	if ftpErr, ok := err.(goftp.Error); ok {
		return ftpErr.Code() == code
	}

	return false
}

func newFTPLogger(address string) *ftpLogger {
	return &ftpLogger{
		address: address,
		logger:  log.New("goftp("+address+")", log.GetLevel()),
	}
}

// simple logger used for the FTP driver debug logging
type ftpLogger struct {
	address string
	logger  log.Logger
}

// Write implements io.Writer.Write
func (l ftpLogger) Write(p []byte) (n int, err error) {
	l.logger.Debug(string(p))
	return len(p), nil
}
