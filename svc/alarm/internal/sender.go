package internal

import (
	"sync"
)

// Sender 接口定义（与主包 alarm.Sender 方法签名相同，可以兼容）
type Sender interface {
	Send(data any) error
}

// BasicSender 内部发送器包装，支持线程安全的存储和加载
type BasicSender struct {
	sender Sender
	mu     sync.RWMutex
}

// NewBasicSender 创建一个新的 BasicSender
func NewBasicSender() *BasicSender {
	return &BasicSender{}
}

func (s *BasicSender) Load() Sender {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sender
}

func (s *BasicSender) Store(v Sender) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sender = v
}
