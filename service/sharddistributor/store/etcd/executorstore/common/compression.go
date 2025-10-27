package common

import (
	"encoding/json"
	"fmt"

	"github.com/golang/snappy"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/types"
)

// Compress compresses data using snappy compression
func Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

// Decompress decompresses snappy-compressed data
func Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	decompressed, err := snappy.Decode(nil, data)
	if err != nil {
		// Decompression failed
		// Return it as-is for backward compatibility
		return nil, fmt.Errorf("failed to decode: %v", err)
	}

	return decompressed, nil
}

func CompressedActiveStatus() string {
	compressed, _ := Compress([]byte(fmt.Sprintf(`"%s"`, types.ExecutorStatusACTIVE)))
	return string(compressed)
}

// DecompressAndUnmarshal decompresses data and unmarshals it into the target
// This function handles both compressed and uncompressed data for backward compatibility
// errorContext is used to provide meaningful error messages
func DecompressAndUnmarshal(data []byte, target interface{}, errorContext string, logger log.Logger) error {
	decompressed, err := Decompress(data)
	if err != nil {
		logger.Warn(fmt.Sprintf("failed to decompress %s, proceeding with unmarshaling..", errorContext), tag.Error(err))
	}
	if err := json.Unmarshal(decompressed, target); err != nil {
		return fmt.Errorf("unmarshal %s: %w", errorContext, err)
	}
	return nil
}
