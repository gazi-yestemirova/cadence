package executorstore

import (
	"encoding/json"
	"fmt"

	"github.com/golang/snappy"

	"github.com/uber/cadence/common/types"
)

// compress compresses data using snappy compression
func compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

// decompress decompresses snappy-compressed data with backward compatibility for uncompressed data
// If decompression fails (indicating it is uncompressed data), return the data as-is
func decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	decompressed, err := snappy.Decode(nil, data)
	if err != nil {
		// Decompression failed
		// Return it as-is for backward compatibility
		return data, nil
	}

	return decompressed, nil
}

func compressedActiveStatus() string {
	compressed, _ := compress([]byte(fmt.Sprintf(`"%s"`, types.ExecutorStatusACTIVE)))
	return string(compressed)
}

// decompressAndUnmarshal decompresses data and unmarshals it into the target
// errorContext is used to provide meaningful error messages
func decompressAndUnmarshal(data []byte, target interface{}, errorContext string) error {
	decompressed, err := decompress(data)
	if err != nil {
		return fmt.Errorf("decompress %s: %w", errorContext, err)
	}
	if err := json.Unmarshal(decompressed, target); err != nil {
		return fmt.Errorf("unmarshal %s: %w", errorContext, err)
	}
	return nil
}
