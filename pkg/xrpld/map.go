package xrpld

import (
	"sync"
)

// SyncMap is a generic thread-safe map implementation.
// It is safe for zero-value usage.
type SyncMap[A comparable, B any] struct {
	data  map[A]B
	mutex sync.RWMutex
}

// Load value for a key. It returns the value and the exists flag.
func (s *SyncMap[A, B]) Load(key A) (B, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

// Store a new key-value pair.
func (s *SyncMap[A, B]) Store(key A, value B) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.data == nil {
		s.data = map[A]B{}
	}
	s.data[key] = value
}

// Delete a key.
func (s *SyncMap[A, B]) Delete(key A) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, key)
}

// Clear the map (remove all entries).
func (s *SyncMap[A, B]) Clear() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data = map[A]B{}
}

// StoreIfAbsent stores the given key-value pair if the key does not already exist.
func (s *SyncMap[A, B]) StoreIfAbsent(key A, value B) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.data == nil {
		s.data = map[A]B{}
	}

	if _, exists := s.data[key]; !exists {
		s.data[key] = value
		return true
	}

	return false
}

// DeleteIf deletes the given key if the given condition evaluates to true.
// The parameters passed to the function are the current value for key and the exists flag.
func (s *SyncMap[A, B]) DeleteIf(key A, cond func(B, bool) bool) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	v, exists := s.data[key]

	if cond(v, exists) {
		delete(s.data, key)
		return true
	}

	return false
}
