package backup

import (
	"errors"
	"io"

	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

const (
	LZ4Compression CompressionType = iota
	XZCompression
)

type CompressionType uint8

// String implements Stringer.String
func (ct *CompressionType) String() string {
	switch *ct {
	case LZ4Compression:
		return lz4CompressionStr
	case XZCompression:
		return xzCompressionStr
	default:
		return ""
	}
}

// Set implements Flag.Set
func (ct *CompressionType) Set(str string) error {
	switch str {
	case lz4CompressionStr:
		*ct = LZ4Compression
	case xzCompressionStr:
		*ct = XZCompression
	default:
		return errUnknownCompressionType
	}

	return nil
}

// Type implements PValue.Type
func (ct *CompressionType) Type() string {
	return "CompressionType"
}

// Validate implements Validator.Validate
func (ct CompressionType) Validate() error {
	switch ct {
	case LZ4Compression, XZCompression:
		return nil
	default:
		return errUnknownCompressionType
	}
}

const (
	lz4CompressionStr = "lz4"
	xzCompressionStr  = "xz"
)

var (
	errUnknownCompressionType = errors.New("unknown compression type")
)

func NewCompressor(ct CompressionType) (Compressor, error) {
	switch ct {
	case LZ4Compression:
		return LZ4Compressor(), nil
	case XZCompression:
		return XZCompressor(), nil
	default:
		return nil, errUnknownCompressionType
	}
}

func NewDecompressor(ct CompressionType) (Decompressor, error) {
	switch ct {
	case LZ4Compression:
		return LZ4Decompressor(), nil
	case XZCompression:
		return XZDecompressor(), nil
	default:
		return nil, errUnknownCompressionType
	}
}

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
