package kafka

import (
	"fmt"
	"strings"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zhuud/go-library/svc/conf"
)

func getServers() ([]string, error) {
	servers := make([]string, 0)

	var clusters map[string]struct {
		Host string `json:"host"`
	}
	_ = conf.GetUnmarshal(fmt.Sprintf("/qconf/web-config/%s", "kafka_cluster"), &clusters)
	if cluster, ok := clusters["tobase"]; ok {
		servers = strings.Split(cluster.Host, ",")
	}
	if len(servers) == 0 {
		c := kq.KqConf{}
		_ = conf.GetUnmarshal("Kafka", &c)
		servers = c.Brokers
	}
	if len(servers) == 0 {
		return servers, fmt.Errorf("kafka.getBrokers not set address, please set env KAFKA_ADDR or config Kafka")
	}
	return servers, nil
}
