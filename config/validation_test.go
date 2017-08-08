package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateBlockSize(t *testing.T) {
	assert := assert.New(t)

	validCases := []int64{
		512,
		1024,
		2048,
		4096,
		8192,
		16384,
		32768,
	}
	for _, validCase := range validCases {
		assert.Truef(ValidateBlockSize(validCase), "%v", validCase)
	}

	invalidCases := []int64{
		0,
		5,
		100,
		300,
		256,
		42,
		560,
		3060,
	}

	for _, invalidCase := range invalidCases {
		assert.Falsef(ValidateBlockSize(invalidCase), "%v", invalidCase)
	}
}
