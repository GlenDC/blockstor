// +build !go1.9

package ardb

import (
	"golang.org/x/sync"
)

type connectionPool sync.Map
type databasePool sync.Map
