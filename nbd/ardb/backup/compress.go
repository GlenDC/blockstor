package backup

import (
	"io"

	"github.com/pierrec/lz4"
)

func Compress(src io.Reader, dst io.Writer) error {
	return LZ4Compressor().Compress(src, dst)
}

func Decompress(src io.Reader, dst io.Writer) error {
	return LZ4Decompressor().Decompress(src, dst)
}

func LZ4Compressor() Compressor {
	return lz4Compressor{}
}

func LZ4Decompressor() Decompressor {
	return lz4Decompressor{}
}

type Compressor interface {
	Compress(src io.Reader, dst io.Writer) error
}

type Decompressor interface {
	Decompress(src io.Reader, dst io.Writer) error
}

type lz4Compressor struct{}

func (c lz4Compressor) Compress(src io.Reader, dst io.Writer) error {
	w := lz4.NewWriter(dst)
	w.Header.BlockChecksum = true

	_, err := w.ReadFrom(src)
	if err != nil {
		return err
	}
	return w.Close()
}

type lz4Decompressor struct{}

func (d lz4Decompressor) Decompress(src io.Reader, dst io.Writer) error {
	r := lz4.NewReader(src)
	r.Header.BlockChecksum = true

	_, err := r.WriteTo(dst)
	return err
}
