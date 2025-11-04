package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/golang/snappy"
)

var (
	// _snappyHeader is a magic prefix prepended to compressed data to distinguish it from uncompressed data
	_snappyHeader = []byte{0xff, 0x06, 0x00, 0x00, 's', 'N', 'a', 'P', 'p', 'Y'}
)

const (
	// CompressionSnappy indicates snappy compression should be applied
	CompressionSnappy = "snappy"
)

// Compress encodes data using the compression library identified by compressionType
// When compressionType is empty or "none", the data is returned as-is
func Compress(data []byte, compressionType string) ([]byte, error) {
	switch strings.ToLower(compressionType) {
	case "", "none":
		return data, nil
	case CompressionSnappy:
		var buf bytes.Buffer
		w := snappy.NewBufferedWriter(&buf)

		if _, err := w.Write(data); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// Decompress decodes snappy-compressed data
// If the snappy header is present, it will successfully decompress it or return an error
// If the snappy header is absent, it treats data as uncompressed and returns it as-is
func Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	if !bytes.HasPrefix(data, _snappyHeader) {
		return data, nil
	}

	r := snappy.NewReader(bytes.NewReader(data))
	decompressed, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return decompressed, nil
}

// DecompressAndUnmarshal decompresses data and unmarshals it into the target
func DecompressAndUnmarshal(data []byte, target interface{}) error {
	decompressed, err := Decompress(data)
	if err != nil {
		return fmt.Errorf("decompress: %w", err)
	}

	if err := json.Unmarshal(decompressed, target); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	return nil
}
