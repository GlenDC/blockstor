package backup

import (
	"errors"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
)

func TestMinimalFTPServerConfigToString(t *testing.T) {
	assert := assert.New(t)

	validCases := []struct {
		Input, Output string
	}{
		{"foo", "ftp://foo:22"},
		{"foo/bar/baz", "ftp://foo:22/bar/baz"},
		{"foo:22", "ftp://foo:22"},
	}

	for _, validCase := range validCases {
		var cfg FTPServerConfig
		if !assert.NoError(cfg.Set(validCase.Input)) {
			continue
		}
		assert.Equal(validCase.Output, cfg.String())
	}
}

func TestFTPServerConfigToString(t *testing.T) {
	assert := assert.New(t)

	validCases := []struct {
		Config   FTPServerConfig
		Expected string
	}{
		{FTPServerConfig{Address: "localhost:2000"}, "ftp://localhost:2000"},
		{FTPServerConfig{Address: "localhost:2000/bar/foo"}, "ftp://localhost:2000/bar/foo"},
		{FTPServerConfig{Address: "localhost:2000/bar"}, "ftp://localhost:2000/bar"},
		{FTPServerConfig{Address: "localhost:2000", Username: "foo"}, "ftp://foo@localhost:2000"},
		{FTPServerConfig{Address: "localhost:2000", Username: "foo", Password: "boo"}, "ftp://foo:boo@localhost:2000"},
		{FTPServerConfig{Address: "localhost:2000/bar", Username: "foo", Password: "boo"}, "ftp://foo:boo@localhost:2000/bar"},
	}

	for _, validCase := range validCases {
		output := validCase.Config.String()
		assert.Equal(validCase.Expected, output)
	}
}

func TestFTPServerConfigStringCommute(t *testing.T) {
	assert := assert.New(t)

	validCases := []string{
		"localhost:2000",
		"localhost:2000",
		"localhost:2000/foo",
		"ftp://localhost:2000",
		"ftp://localhost:2000/foo",
		"username@localhost:2000",
		"username@localhost:200/foo0",
		"ftp://username@localhost:2000/bar/foo",
		"user:pass@localhost:3000",
		"user:pass@localhost:3000/bar",
		"ftp://user:pass@localhost:3000/bar",
	}

	for _, validCase := range validCases {
		var cfg FTPServerConfig
		if !assert.NoError(cfg.Set(validCase)) {
			continue
		}

		expected := validCase
		if !strings.HasPrefix(expected, "ftp://") {
			expected = "ftp://" + expected
		}
		assert.Equal(expected, cfg.String())
	}
}

// newStubDriver creates an in-memory Server Driver,
// which is to be used for testing purposes only.
func newStubDriver() *stubDriver {
	return &stubDriver{
		dedupedBlocks: make(map[string][]byte),
		dedupedMaps:   make(map[string][]byte),
	}
}

type stubDriver struct {
	dedupedBlocks map[string][]byte
	dedupedMaps   map[string][]byte

	bmux, mmux sync.RWMutex
}

// SetDedupedBlock implements ServerDriver.SetDedupedBlock
func (stub *stubDriver) SetDedupedBlock(hash zerodisk.Hash, r io.Reader) error {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	stub.bmux.Lock()
	defer stub.bmux.Unlock()
	stub.dedupedBlocks[string(hash)] = bytes
	return nil
}

// SetDedupedMap implements ServerDriver.SetDedupedMap
func (stub *stubDriver) SetDedupedMap(id string, r io.Reader) error {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	stub.mmux.Lock()
	defer stub.mmux.Unlock()
	stub.dedupedMaps[id] = bytes
	return nil
}

// GetDedupedBlock implements ServerDriver.GetDedupedBlock
func (stub *stubDriver) GetDedupedBlock(hash zerodisk.Hash, w io.Writer) error {
	stub.bmux.RLock()
	defer stub.bmux.RUnlock()

	bytes, ok := stub.dedupedBlocks[string(hash)]
	if !ok {
		return ErrDataDidNotExist
	}
	n, err := w.Write(bytes)
	if err != nil {
		return err
	}
	if n != len(bytes) {
		return errors.New("couldn't write full block")
	}
	return nil
}

// GetDedupedMap implements ServerDriver.GetDedupedMap
func (stub *stubDriver) GetDedupedMap(id string, w io.Writer) error {
	stub.mmux.RLock()
	defer stub.mmux.RUnlock()

	bytes, ok := stub.dedupedMaps[id]
	if !ok {
		return ErrDataDidNotExist
	}
	n, err := w.Write(bytes)
	if err != nil {
		return err
	}
	if n != len(bytes) {
		return errors.New("couldn't write full deduped map")
	}
	return nil
}

// Close implements ServerDriver.Close
func (stub *stubDriver) Close() error {
	return nil // nothing to do
}
