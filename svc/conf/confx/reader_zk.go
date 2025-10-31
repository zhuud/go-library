package confx

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/zhuud/go-library/svc/conf"
	"github.com/zhuud/go-library/svc/zookeeper"
)

type zookeeperReader struct {
	handler *zookeeper.Client
}

var (
	zr *zookeeperReader

	rmu sync.RWMutex
)

func NewZookeeperReader(zk *zookeeper.Client) conf.Reader {
	if zr != nil {
		return zr
	}
	rmu.Lock()
	defer rmu.Unlock()

	return &zookeeperReader{
		handler: zk,
	}
}

func (r *zookeeperReader) Get(k string) (string, error) {
	v, err := r.handler.GetC(k)
	if err != nil {
		return "", fmt.Errorf("confx.zookeeperReader.Get error: %w", err)
	}
	return v, nil
}

func (r *zookeeperReader) GetAny(k string, target any) error {
	if len(k) == 0 {
		return fmt.Errorf("confx.zookeeperReader k empty")
	}

	v, err := r.Get(k)
	if err != nil {
		return fmt.Errorf("confx.zookeeperReader.Get error: %w", err)
	}
	if len(v) == 0 {
		return fmt.Errorf("confx.zookeeperReader.Get nil")
	}

	err = json.Unmarshal([]byte(v), target)
	if err != nil {
		return fmt.Errorf("confx.zookeeperReader.Unmarshal error: %w", err)
	}
	return nil
}
