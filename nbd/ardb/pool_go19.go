package ardb

import (
	"sync"
)

type connectionPool = sync.Map
type databasePool = sync.Map
