package backup

import (
	"bytes"
	"io/ioutil"

	"github.com/zero-os/0-Disk"
)

type Importer struct {
	// TODO
}

// fetch -> decrypt -> decompress
type importPipeline struct {
	ServerDriver ServerDriver
	Decrypter    Decrypter
	Decompressor Decompressor
}

func (p *importPipeline) ReadBlock(hash zerodisk.Hash) ([]byte, error) {
	bufA := bytes.NewBuffer(nil)
	err := p.ServerDriver.GetDedupedBlock(hash, bufA)
	if err != nil {
		return nil, err
	}

	bufB := bytes.NewBuffer(nil)
	err = p.Decrypter.Decrypt(bufA, bufB)
	if err != nil {
		return nil, err
	}

	bufA.Reset()
	err = p.Decompressor.Decompress(bufB, bufA)
	if err != nil {
		return nil, err
	}

	bytes, err := ioutil.ReadAll(bufA)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}
