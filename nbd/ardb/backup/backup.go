package backup

type ServerDriver interface {
	SetDedupedBlock(hash, data []byte) error
	SetDedupedMap(id string, dm *DedupedMap) error

	GetDedupedBlock(hash []byte) ([]byte, error)
	GetDedupedMap(id string) (*DedupedMap, error)
}
