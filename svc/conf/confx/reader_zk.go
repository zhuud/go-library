package confx

import (
	"encoding/json"
	"sync"

	"github.com/pkg/errors"
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

func (r *zookeeperReader) Get(k string, dv ...string) (string, error) {
	v, err := r.handler.GetC(k)
	if err != nil {
		return "", errors.Wrap(err, "confx.zookeeperReader.Get error")
	}
	if len(v) == 0 && len(dv) > 0 {
		return dv[0], nil
	}
	return v, nil
}

func (r *zookeeperReader) GetAny(k string, target any) error {
	if len(k) == 0 {
		return errors.New("confx.zookeeperReader k empty")
	}

	v, err := r.Get(k)
	if err != nil {
		return errors.Wrap(err, "confx.zookeeperReader.Get error")
	}
	if len(v) == 0 {
		return errors.New("confx.zookeeperReader.Get nil")
	}

	err = json.Unmarshal([]byte(v), target)
	return errors.Wrap(err, "confx.zookeeperReader.Unmarshal error")
}
