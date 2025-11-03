package common

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecompress(t *testing.T) {
	t.Run("Empty data", func(t *testing.T) {
		decompressed, err := Decompress([]byte{})
		require.NoError(t, err)
		assert.Empty(t, decompressed)
	})

	t.Run("Nil data", func(t *testing.T) {
		decompressed, err := Decompress(nil)
		require.NoError(t, err)
		assert.Nil(t, decompressed)
	})

	t.Run("Uncompressed data", func(t *testing.T) {
		uncompressed := []byte(`{"status":"ACTIVE"}`)

		result, err := Decompress(uncompressed)
		require.NoError(t, err)
		assert.Equal(t, uncompressed, result, "Uncompressed data is returned as-is")

		var status map[string]string
		err = json.Unmarshal(result, &status)
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", status["status"])
	})

	t.Run("Compressed data", func(t *testing.T) {
		original := []byte(`{"status":"DRAINING"}`)
		compressed, err := compressData(original)
		require.NoError(t, err)

		result, err := Decompress(compressed)
		require.NoError(t, err)
		assert.Equal(t, original, result)

		var status map[string]string
		err = json.Unmarshal(result, &status)
		require.NoError(t, err)
		assert.Equal(t, "DRAINING", status["status"])
	})
}

func TestDecompressAndUnmarshal(t *testing.T) {
	type testData struct {
		Status string   `json:"status"`
		Shards []string `json:"shards"`
	}

	t.Run("Uncompressed data", func(t *testing.T) {
		data := []byte(`{"status":"ACTIVE","shards":["shard1","shard2"]}`)

		var result testData
		err := DecompressAndUnmarshal(data, &result, "test data")
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", result.Status)
		assert.Equal(t, []string{"shard1", "shard2"}, result.Shards)
	})

	t.Run("Compressed data", func(t *testing.T) {
		original := testData{
			Status: "DRAINING",
			Shards: []string{"shard3", "shard4"},
		}
		originalJSON, _ := json.Marshal(original)
		compressed, err := compressData(originalJSON)
		require.NoError(t, err)

		var result testData
		err = DecompressAndUnmarshal(compressed, &result, "test data")
		require.NoError(t, err)
		assert.Equal(t, original.Status, result.Status)
		assert.Equal(t, original.Shards, result.Shards)
	})

	t.Run("Invalid JSON in uncompressed data", func(t *testing.T) {
		invalidJSON := []byte(`{invalid json}`)

		var result testData
		err := DecompressAndUnmarshal(invalidJSON, &result, "test data")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal test data")
	})
}

func TestHasFramedHeader(t *testing.T) {
	t.Run("Data with header", func(t *testing.T) {
		data := append(snappyMagic, []byte("some data")...)
		assert.True(t, hasFramedHeader(data))
	})

	t.Run("Data without header", func(t *testing.T) {
		data := []byte(`{"json":"data"}`)
		assert.False(t, hasFramedHeader(data))
	})

	t.Run("Empty data", func(t *testing.T) {
		assert.False(t, hasFramedHeader([]byte{}))
	})

	t.Run("Data shorter than header", func(t *testing.T) {
		assert.False(t, hasFramedHeader([]byte{0xff, 0x06}))
	})
}

func compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := snappy.NewBufferedWriter(&buf)

	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
