package alarm

import (
	"sync"
)

type (
	Sender interface {
		Send(data any) error
	}

	basicSender struct {
		sender Sender
		mu     sync.RWMutex
	}
)

var (
	smu sync.RWMutex
)

func (s *basicSender) Load() Sender {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sender
}

func (s *basicSender) Store(v Sender) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sender = v
}
