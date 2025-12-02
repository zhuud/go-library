package conf

import (
	"encoding/json"
	"fmt"
	"sync"
)

// zookeeperClient Zookeeper 客户端接口（避免循环依赖，不暴露给包外部）
type zookeeperClient interface {
	GetC(k string) (string, error)
}

type zookeeperReader struct {
	handler zookeeperClient
}

var (
	zr   *zookeeperReader
	zrMu sync.Mutex
)

// NewZookeeperReader 创建 Zookeeper 读取器
func NewZookeeperReader(zk zookeeperClient) Reader {
	if zr != nil {
		return zr
	}
	zrMu.Lock()
	defer zrMu.Unlock()

	// 双重检查
	if zr != nil {
		return zr
	}

	zr = &zookeeperReader{
		handler: zk,
	}
	return zr
}

func (r *zookeeperReader) Get(k string) (string, error) {
	v, err := r.handler.GetC(k)
	if err != nil {
		return "", fmt.Errorf("conf.zookeeperReader.Get error: %w", err)
	}
	return v, nil
}

func (r *zookeeperReader) GetAny(k string, target any) error {
	if len(k) == 0 {
		return fmt.Errorf("conf.zookeeperReader k empty")
	}

	v, err := r.Get(k)
	if err != nil {
		return fmt.Errorf("conf.zookeeperReader.Get error: %w", err)
	}
	if len(v) == 0 {
		return fmt.Errorf("conf.zookeeperReader.Get nil")
	}

	err = json.Unmarshal([]byte(v), target)
	if err != nil {
		return fmt.Errorf("conf.zookeeperReader.Unmarshal error: %w", err)
	}
	return nil
}

func (r *zookeeperReader) Name() string {
	return "zookeeper"
}
