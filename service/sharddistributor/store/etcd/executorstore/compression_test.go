package executorstore

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/types"
)

func TestCompressDecompress(t *testing.T) {
	original := []byte(`{"status":"ACTIVE","shards":["shard1","shard2"]}`)

	compressed, err := compress(original)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	assert.NotEqual(t, original, compressed)

	decompressed, err := decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestDecompressBackwardCompatibility(t *testing.T) {
	t.Run("Old uncompressed data", func(t *testing.T) {
		oldData := []byte(`{"status":"ACTIVE"}`)

		result, err := decompress(oldData)
		require.NoError(t, err)
		assert.Equal(t, oldData, result, "Old uncompressed data should be returned as-is")

		var status map[string]string
		err = json.Unmarshal(result, &status)
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", status["status"])
	})

	t.Run("New compressed data", func(t *testing.T) {
		original := []byte(`{"status":"DRAINING"}`)
		compressed, err := compress(original)
		require.NoError(t, err)

		result, err := decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, original, result)

		var status map[string]string
		err = json.Unmarshal(result, &status)
		require.NoError(t, err)
		assert.Equal(t, "DRAINING", status["status"])
	})
}

func TestDecompressAndUnmarshalBackwardCompatibility(t *testing.T) {
	type testData struct {
		Status string   `json:"status"`
		Shards []string `json:"shards"`
	}

	t.Run("Old uncompressed JSON", func(t *testing.T) {
		oldData := []byte(`{"status":"ACTIVE","shards":["shard1","shard2"]}`)

		var result testData
		err := decompressAndUnmarshal(oldData, &result, "test data")
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", result.Status)
		assert.Equal(t, []string{"shard1", "shard2"}, result.Shards)
	})

	t.Run("New compressed data", func(t *testing.T) {
		original := testData{
			Status: "DRAINING",
			Shards: []string{"shard3", "shard4"},
		}
		originalJSON, _ := json.Marshal(original)
		compressed, err := compress(originalJSON)
		require.NoError(t, err)

		var result testData
		err = decompressAndUnmarshal(compressed, &result, "test data")
		require.NoError(t, err)
		assert.Equal(t, original.Status, result.Status)
		assert.Equal(t, original.Shards, result.Shards)
	})

	t.Run("Invalid JSON in uncompressed data", func(t *testing.T) {
		invalidJSON := []byte(`{invalid json}`)

		var result testData
		err := decompressAndUnmarshal(invalidJSON, &result, "test data")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal test data")
	})
}

func TestCompressedActiveStatus(t *testing.T) {
	compressed := compressedActiveStatus()
	require.NotEmpty(t, compressed)

	decompressed, err := decompress([]byte(compressed))
	require.NoError(t, err)

	var status types.ExecutorStatus
	err = json.Unmarshal(decompressed, &status)
	require.NoError(t, err)
	assert.Equal(t, types.ExecutorStatusACTIVE, status)
}
