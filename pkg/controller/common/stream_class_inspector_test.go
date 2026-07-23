package common

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCachedStreamClassInspector(t *testing.T) {
	t.Run("cache miss calls inspector", func(t *testing.T) {
		var callCount int
		inspector := NewCachedStreamClassInspector(func(url string) (string, error) {
			callCount++
			return "rhel-10", nil
		})

		sc, err := inspector.Inspect("quay.io/image@sha256:abc")
		require.NoError(t, err)
		assert.Equal(t, "rhel-10", sc)
		assert.Equal(t, 1, callCount)
	})

	t.Run("cache hit skips inspector", func(t *testing.T) {
		var callCount int
		inspector := NewCachedStreamClassInspector(func(url string) (string, error) {
			callCount++
			return "rhel-9", nil
		})

		_, err := inspector.Inspect("quay.io/image@sha256:abc")
		require.NoError(t, err)

		sc, err := inspector.Inspect("quay.io/image@sha256:abc")
		require.NoError(t, err)
		assert.Equal(t, "rhel-9", sc)
		assert.Equal(t, 1, callCount)
	})

	t.Run("different URLs are cached independently", func(t *testing.T) {
		var callCount int
		inspector := NewCachedStreamClassInspector(func(url string) (string, error) {
			callCount++
			if url == "quay.io/a" {
				return "rhel-9", nil
			}
			return "rhel-10", nil
		})

		sc1, err := inspector.Inspect("quay.io/a")
		require.NoError(t, err)
		assert.Equal(t, "rhel-9", sc1)

		sc2, err := inspector.Inspect("quay.io/b")
		require.NoError(t, err)
		assert.Equal(t, "rhel-10", sc2)
		assert.Equal(t, 2, callCount)

		_, _ = inspector.Inspect("quay.io/a")
		_, _ = inspector.Inspect("quay.io/b")
		assert.Equal(t, 2, callCount)
	})

	t.Run("error is not cached", func(t *testing.T) {
		var callCount int
		inspector := NewCachedStreamClassInspector(func(url string) (string, error) {
			callCount++
			if callCount == 1 {
				return "", fmt.Errorf("network error")
			}
			return "rhel-10", nil
		})

		_, err := inspector.Inspect("quay.io/image@sha256:abc")
		require.Error(t, err)
		assert.Equal(t, 1, callCount)

		sc, err := inspector.Inspect("quay.io/image@sha256:abc")
		require.NoError(t, err)
		assert.Equal(t, "rhel-10", sc)
		assert.Equal(t, 2, callCount)
	})

	t.Run("empty stream class is not cached", func(t *testing.T) {
		var callCount int
		inspector := NewCachedStreamClassInspector(func(url string) (string, error) {
			callCount++
			if callCount == 1 {
				return "", nil
			}
			return "rhel-10", nil
		})

		sc, err := inspector.Inspect("quay.io/image@sha256:abc")
		require.NoError(t, err)
		assert.Equal(t, "", sc)

		sc, err = inspector.Inspect("quay.io/image@sha256:abc")
		require.NoError(t, err)
		assert.Equal(t, "rhel-10", sc)
		assert.Equal(t, 2, callCount)
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		var callCount atomic.Int32
		inspector := NewCachedStreamClassInspector(func(url string) (string, error) {
			callCount.Add(1)
			return "rhel-10", nil
		})

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sc, err := inspector.Inspect("quay.io/image@sha256:abc")
				assert.NoError(t, err)
				assert.Equal(t, "rhel-10", sc)
			}()
		}
		wg.Wait()
	})
}
