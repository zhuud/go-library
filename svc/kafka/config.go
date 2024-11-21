package kafka

import (
	"errors"
	"github.com/zeromicro/go-queue/kq"
	"github.com/zhuud/go-library/svc/conf"
)

func getServers() ([]string, error) {
	servers := make([]string, 0)

	_ = conf.GetUnmarshal("KAFKA_ADDR", &servers)
	if len(servers) == 0 {
		c := kq.KqConf{}
		_ = conf.GetUnmarshal("Kafka", &c)
		servers = c.Brokers
	}
	if len(servers) == 0 {
		return servers, errors.New("kafka.getBrokers not set address, please set env KAFKA_ADDR or config Kafka")
	}
	return servers, nil
}
