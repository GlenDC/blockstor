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

	RootDir string // optional
}

// String implements Stringer.String
func (cfg *FTPServerConfig) String() string {
	if cfg.Validate() != nil {
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
func (cfg *FTPServerConfig) Set(str string) error {
	parts := ftpURLRegexp.FindStringSubmatch(str)
	if len(parts) != 6 {
		return fmt.Errorf("'%s' is not a valid FTP URL", str)
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

	// all info was set correctly
	return nil
}

// Type implements PFlag.Type
func (cfg *FTPServerConfig) Type() string {
	return "FTPServerConfig"
}

// ftpURLRegexp is a simplistic ftp URL regexp,
// to be able to split an FTP url into credentials and the hostname.
// It doesn't contain much validation, as that isn't the point of this regexp.
var ftpURLRegexp = regexp.MustCompile(`^(?:ftp://)?(?:([^:@]+)(?::([^:@]+))?@)?(?:([^@/:]+)(:[0-9]+)?(/.*)?)$`)

// Validate the FTP Server Config.
func (cfg *FTPServerConfig) Validate() error {
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
		Logger:             newFTPLogger(cfg.Address),
	}

	client, err := goftp.DialConfig(config, cfg.Address)
	if err != nil {
		return nil, err
	}

	ftpDriver := &ftpDriver{
		client:    client,
		knownDirs: newDirCache(),
		rootDir:   strings.Trim(cfg.RootDir, "/"),
	}

	return ftpDriver, nil
}

type ftpDriver struct {
	client    *goftp.Client
	knownDirs *dirCache
	rootDir   string

	fileExistsMux sync.Mutex
	fileExists    func(string) bool
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

	loc := path.Join(ftp.rootDir, dir, file)
	return ftp.retrieve(loc, w)
}

// GetDedupedMap implements ServerDriver.GetDedupedMap
func (ftp *ftpDriver) GetDedupedMap(id string, w io.Writer) error {
	loc := path.Join(ftp.rootDir, backupDir, id)
	fmt.Printf("%q\n", loc)
	return ftp.retrieve(loc, w)
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

		// get the info about the current level (if it exists at all)
		info, err := ftp.client.Stat(pwd)
		// check if have to still create the current level
		if isFTPErrorCode(ftpErrorNoExists, err) || isFTPErrorCode(ftpErrorInvalidCommand, err) {
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
func (ftp *ftpDriver) retrieve(path string, dest io.Writer) error {
	err := ftp.client.Retrieve(path, dest)
	if isFTPErrorCode(ftpErrorNoExists, err) {
		return ErrDataDidNotExist
	}
	return err
}

// TODO: test this function

func hashAsDirAndFile(hash zerodisk.Hash) (string, string, bool) {
	if len(hash) != zerodisk.HashSize {
		return "", "", false
	}

	dir := hashBytesToString(hash[:2]) + "/" + hashBytesToString(hash[2:4])
	file := hashBytesToString(hash[4:])
	return dir, file, true
}

// TODO: test this function

func hashBytesToString(bs []byte) string {
	var str string
	for _, b := range bs {
		str += hexadecimals[b/16] + hexadecimals[b%16]
	}
	return str
}

var hexadecimals = map[byte]string{
	0:  "0",
	1:  "1",
	2:  "2",
	3:  "3",
	4:  "4",
	5:  "5",
	6:  "6",
	7:  "7",
	8:  "8",
	9:  "9",
	10: "A",
	11: "B",
	12: "C",
	13: "D",
	14: "E",
	15: "F",
}

// list of ftp error codes we care about
const (
	ftpErrorInvalidCommand = 500
	ftpErrorNoExists       = 550
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
