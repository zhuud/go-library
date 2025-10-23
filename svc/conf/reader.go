package conf

import (
	"sync"
)

type (
	Reader interface {
		Get(k string) (string, error)
		GetAny(k string, target any) error
	}

	basicReader struct {
		reader Reader
		mu     sync.RWMutex
	}
)

var (
	rmu sync.RWMutex
)

func (r *basicReader) Load() Reader {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reader
}

func (r *basicReader) Store(v Reader) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reader = v
}
