package rippled

import (
	"maps"
	"sync"
)

type SyncMap[A comparable, B any] struct {
	data  map[A]B
	mutex sync.RWMutex
}

func (s *SyncMap[A, B]) Load(key A) (B, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func (s *SyncMap[A, B]) Store(key A, value B) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.data == nil {
		s.data = map[A]B{}
	}
	s.data[key] = value
}

func (s *SyncMap[A, B]) Delete(key A) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, key)
}

func (s *SyncMap[A, B]) Clear() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data = map[A]B{}
}

func (s *SyncMap[A, B]) Range(op func(A, B)) {
	s.mutex.RLock()
	clone := maps.Clone(s.data)
	s.mutex.RUnlock()

	for key, value := range clone {
		op(key, value)
	}
}
