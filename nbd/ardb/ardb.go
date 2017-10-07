package ardb

import (
	"io"
	"net"

	"github.com/zero-os/0-Disk/log"
)

// shared constants
const (
	// DefaultLBACacheLimit defines the default cache limit
	DefaultLBACacheLimit = 20 * MebibyteAsBytes // 20 MiB
	// constants used to convert between MiB/GiB and bytes
	GibibyteAsBytes int64 = 1024 * 1024 * 1024
	MebibyteAsBytes int64 = 1024 * 1024
)

// MapErrorToBroadcastStatus tries to map the given error,
// returned by a `Connection` operation to a broadcast's message status.
func MapErrorToBroadcastStatus(err error) (log.MessageStatus, bool) {
	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			return log.StatusServerTimeout, true
		}
		if netErr.Temporary() {
			return log.StatusServerTempError, true
		}

		return log.StatusUnknownError, true
	}

	if err == io.EOF {
		return log.StatusServerDisconnect, true
	}

	return 0, false
}
