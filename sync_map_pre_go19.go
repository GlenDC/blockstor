// +build !go1.9

package zerodisk

import (
	"golang.org/x/sync"
)

type SyncMap sync.Map
