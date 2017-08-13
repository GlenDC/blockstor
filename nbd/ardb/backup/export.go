package backup

import (
	"bytes"

	"github.com/zero-os/0-Disk"
)

type Exporter struct {
	// TODO
}

// compress -> encrypt -> store
type exportPipeline struct {
	Compressor   Compressor
	Encrypter    Encrypter
	ServerDriver ServerDriver
}

func (p *exportPipeline) WriteBlock(hash zerodisk.Hash, data []byte) error {
	bufA := bytes.NewBuffer(data)
	bufB := bytes.NewBuffer(nil)

	err := p.Compressor.Compress(bufA, bufB)
	if err != nil {
		return err
	}

	bufA = bytes.NewBuffer(nil)
	err = p.Encrypter.Encrypt(bufB, bufA)
	if err != nil {
		return err
	}

	return p.ServerDriver.SetDedupedBlock(hash, bufA)
}
