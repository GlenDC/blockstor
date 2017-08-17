package config

// ValidateBlockSize allows you to validate a block size,
// returning true if the given block size is valid.
func ValidateBlockSize(bs int64) bool {
	return bs >= 512 && (bs&(bs-1)) == 0
}
