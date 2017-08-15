package backup

import (
	"io"

	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

func LZ4Compressor() Compressor {
	return lz4Compressor{}
}

func LZ4Decompressor() Decompressor {
	return lz4Decompressor{}
}

func XZCompressor() Compressor {
	return xzCompressor{}
}

func XZDecompressor() Decompressor {
	return xzDecompressor{}
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
		w.Close()
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

type xzCompressor struct{}

func (c xzCompressor) Compress(src io.Reader, dst io.Writer) error {
	w, err := xz.NewWriter(dst)
	if err != nil {
		return err
	}

	_, err = io.Copy(w, src)
	if err != nil {
		w.Close()
		return err
	}

	return w.Close()
}

type xzDecompressor struct{}

func (d xzDecompressor) Decompress(src io.Reader, dst io.Writer) error {
	r, err := xz.NewReader(src)
	if err != nil {
		return err
	}

	_, err = io.Copy(dst, r)
	return err
}
