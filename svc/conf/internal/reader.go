package internal

import (
	"sync"
)

// Reader 配置读取器接口
type Reader interface {
	Get(k string) (string, error)
	GetAny(k string, target any) error
	Name() string // 返回读取器名称
}

// BasicReader 基础读取器包装，支持线程安全的存储和加载
type BasicReader struct {
	reader Reader
	mu     sync.RWMutex
}

// NewBasicReader 创建一个新的 BasicReader
func NewBasicReader() *BasicReader {
	return &BasicReader{}
}

func (r *BasicReader) Load() Reader {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reader
}

func (r *BasicReader) Store(v Reader) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reader = v
}
