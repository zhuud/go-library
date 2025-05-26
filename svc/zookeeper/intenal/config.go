package intenal

import (
	"errors"
	"github.com/zhuud/go-library/svc/conf"
)

func GetServers() ([]string, error) {
	servers := make([]string, 0)

	_ = conf.GetUnmarshal("ZK_ADDR", &servers)
	if len(servers) == 0 {
		_ = conf.GetUnmarshal("ZkAddr", &servers)
	}
	if len(servers) == 0 {
		return servers, errors.New("zookeeper.NewZookeeperClient not set address, please set env ZK_ADDR or config ZkAddr")
	}
	return servers, nil
}
