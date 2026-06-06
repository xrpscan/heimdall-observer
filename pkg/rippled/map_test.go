package rippled

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncMap_StoreAndLoad(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]
	m.Store("a", 1)

	v, ok := m.Load("a")
	require.True(t, ok)
	require.Equal(t, 1, v)
}

func TestSyncMap_LoadMissing(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]

	v, ok := m.Load("missing")
	require.False(t, ok)
	require.Equal(t, 0, v)
}

func TestSyncMap_StoreOverwrites(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]
	m.Store("a", 1)
	m.Store("a", 2)

	v, ok := m.Load("a")
	require.True(t, ok)
	require.Equal(t, 2, v)
}

func TestSyncMap_Delete(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]
	m.Store("a", 1)
	m.Delete("a")

	_, ok := m.Load("a")
	require.False(t, ok)
}

func TestSyncMap_DeleteMissing(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]
	m.Delete("missing")
}

func TestSyncMap_Clear(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]
	m.Store("a", 1)
	m.Store("b", 2)
	m.Clear()

	_, ok := m.Load("a")
	require.False(t, ok)
	_, ok = m.Load("b")
	require.False(t, ok)
}

func TestSyncMap_StoreIfAbsent(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]

	ok := m.StoreIfAbsent("a", 1)
	require.True(t, ok)

	v, exists := m.Load("a")
	require.True(t, exists)
	require.Equal(t, 1, v)

	ok = m.StoreIfAbsent("a", 2)
	require.False(t, ok)

	v, exists = m.Load("a")
	require.True(t, exists)
	require.Equal(t, 1, v)
}

func TestSyncMap_DeleteIf_ConditionTrue(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]
	m.Store("a", 1)

	deleted := m.DeleteIf("a", func(v int, exists bool) bool {
		return exists && v == 1
	})
	require.True(t, deleted)

	_, ok := m.Load("a")
	require.False(t, ok)
}

func TestSyncMap_DeleteIf_ConditionFalse(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]
	m.Store("a", 1)

	deleted := m.DeleteIf("a", func(v int, exists bool) bool {
		return v == 99
	})
	require.False(t, deleted)

	_, ok := m.Load("a")
	require.True(t, ok)
}

func TestSyncMap_DeleteIf_MissingKey(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]

	deleted := m.DeleteIf("missing", func(v int, exists bool) bool {
		return exists
	})
	require.False(t, deleted)
}

func TestSyncMap_ZeroValueSafety(t *testing.T) {
	t.Parallel()

	var m SyncMap[string, int]

	m.Load("x")
	m.Delete("x")
	m.Clear()
}

func TestSyncMap_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	var m SyncMap[int, int]
	var wg sync.WaitGroup

	for i := range 100 {
		wg.Go(func() {
			m.Store(i, i)
			m.Load(i)
			m.Delete(i)
			m.StoreIfAbsent(i, i)
			m.DeleteIf(i, func(_ int, exists bool) bool { return exists })
		})
	}

	wg.Wait()
}
