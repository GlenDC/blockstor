package config

import (
	"net"

	valid "github.com/asaskevich/govalidator"
)

// ValidateBlockSize allows you to validate a block size,
// returning true if the given block size is valid.
func ValidateBlockSize(bs int64) bool {
	return bs >= 512 && (bs&(bs-1)) == 0
}

// IsIPWithOptionalPort returns true if the given str is an IP (with optional Port).
func IsIPWithOptionalPort(str string) bool {
	if h, p, err := net.SplitHostPort(str); err == nil {
		if !validateHost(h) {
			return false
		}

		return p == "" || valid.IsPort(p)
	}

	return validateHost(str)
}

func validateHost(h string) bool {
	if h == "" {
		return false
	}

	return valid.IsDNSName(h) || valid.IsIP(h)
}
