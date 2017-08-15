package backup

import (
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"

	"github.com/secsy/goftp"
)

// ServerDriver defines the API of a driver,
// which allows us to read/write from/to a server,
// the deduped blocks and map which form a backup.
type ServerDriver interface {
	SetDedupedBlock(hash zerodisk.Hash, r io.Reader) error
	SetDedupedMap(id string, r io.Reader) error

	GetDedupedBlock(hash zerodisk.Hash, w io.Writer) error
	GetDedupedMap(id string, w io.Writer) error

	Close() error
}

// FTPServerConfig is used to configure and create an FTP Driver.
type FTPServerConfig struct {
	// Address of the FTP Server
	Address string

	// Optional: username of Authorized account
	//           for the given FTP server
	Username string
	// Optional: password of Authorized account
	//           for the given FTP server
	Password string

	// Optional: Root directory to store backups on.
	//           Only if the default FTP dir is not desired.
	RootDir string
}

// Validate the FTP Server Config.
// TODO: validate more in depth.
func (cfg *FTPServerConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	if cfg.Password != "" && cfg.Username == "" {
		return errors.New("password given, while username is missing")
	}

	return nil
}

// FTPDriver ceates a driver which allows you
// to read/write deduped blocks/map from/to a FTP server.
func FTPDriver(cfg FTPServerConfig) (ServerDriver, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	config := goftp.Config{
		User:               cfg.Username,
		Password:           cfg.Password,
		ConnectionsPerHost: 10,
		Timeout:            10 * time.Second,
		Logger:             &ftpLogger{cfg.Address},
	}

	client, err := goftp.DialConfig(config, cfg.Address)
	if err != nil {
		return nil, err
	}

	return &ftpDriver{
		client:    client,
		root:      cfg.RootDir,
		knownDirs: newDirCache(),
	}, nil
}

type ftpDriver struct {
	client    *goftp.Client
	root      string
	knownDirs *dirCache
}

// SetDedupedBlock implements ServerDriver.SetDedupedBlock
func (ftp *ftpDriver) SetDedupedBlock(hash zerodisk.Hash, r io.Reader) error {
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
func (ftp *ftpDriver) SetDedupedMap(id string, r io.Reader) error {
	dir, err := ftp.mkdirs(backupDir)
	if err != nil {
		return err
	}

	return ftp.lazyStore(path.Join(dir, id), r)
}

// GetDedupedBlock implements ServerDriver.GetDedupedBlock
func (ftp *ftpDriver) GetDedupedBlock(hash zerodisk.Hash, w io.Writer) error {
	dir, file, ok := hashAsDirAndFile(hash)
	if !ok {
		return errInvalidHash
	}

	return ftp.client.Retrieve(path.Join(dir, file), w)
}

// GetDedupedMap implements ServerDriver.GetDedupedMap
func (ftp *ftpDriver) GetDedupedMap(id string, w io.Writer) error {
	return ftp.client.Retrieve(path.Join(backupDir, id), w)
}

// Close implements ServerDriver.Close
func (ftp *ftpDriver) Close() error {
	return ftp.client.Close()
}

// mkdirs walks through the given directory, one level at a time.
// If a level does not exists yet, it will be created.
// An error is returned if any of the used FTP commands returns an error,
// or in case a file at some point already exist, but is not a directory.
// The returned string is how the client should refer to the created directory.
func (ftp *ftpDriver) mkdirs(dir string) (string, error) {
	dir = path.Join(ftp.root, dir)

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

		// get the info about the current level (if it exists at all)
		info, err := ftp.client.Stat(pwd)
		// check if have to still create the current level
		if isFTPErrorCode(ftpErrorNoExists, err) {
			// create the current (sub) directory
			output, err := ftp.client.Mkdir(pwd)
			if err != nil {
				return "", err
			}
			// use returned path
			// see: http://godoc.org/github.com/secsy/goftp#Client.Mkdir
			pwd = output
			continue
		}
		// if we have any other kind of error,
		// simply quit here, as we don't know how to handle that
		if err != nil {
			return "", err
		}
		// ensure that the current (already existing) file,
		// actually is a directory
		if !info.IsDir() {
			return "", errors.New(pwd + " exists and is a file, not a dir")
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
func (ftp *ftpDriver) lazyStore(path string, r io.Reader) error {
	// get info about the given path
	info, err := ftp.client.Stat(path)
	if err != nil {
		// if an error is received,
		// let's check if simply telling us the file doesn't exist yet,
		// if so, let's create it now.
		if isFTPErrorCode(ftpErrorNoExists, err) {
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

func hashAsDirAndFile(hash zerodisk.Hash) (string, string, bool) {
	if len(hash) != zerodisk.HashSize {
		return "", "", false
	}

	dir := string(hash[:2]) + "/" + string(hash[2:4])
	file := string(hash[4:])
	return dir, file, true
}

// list of ftp error codes we care about
const (
	ftpErrorNoExists = 550
)

// small util function which allows us to check if an error
// is an FTP error, and if so, if it's the code we are looking for.
func isFTPErrorCode(code int, err error) bool {
	if ftpErr, ok := err.(goftp.Error); ok {
		return ftpErr.Code() == code
	}

	return false
}

// simple logger used for the FTP driver debug logging
type ftpLogger struct {
	address string
}

// Write implements io.Writer.Write
func (l ftpLogger) Write(p []byte) (n int, err error) {
	log.Debugf("FTP Server (%s): %s", l.address, string(p))
	return len(p), nil
}

func newDirCache() *dirCache {
	return &dirCache{knownDirs: make(map[string]struct{})}
}

type dirCache struct {
	knownDirs map[string]struct{}
	mux       sync.RWMutex
}

func (cache *dirCache) AddDir(path string) {
	cache.mux.Lock()
	defer cache.mux.Unlock()
	cache.knownDirs[path] = struct{}{}
}

func (cache *dirCache) CheckDir(path string) bool {
	cache.mux.RLock()
	defer cache.mux.RUnlock()
	_, exists := cache.knownDirs[path]
	return exists
}

const (
	backupDir = "backups"
)

// some static errors returned by this file's API
var (
	errInvalidHash = errors.New("invalid hash given")
)
