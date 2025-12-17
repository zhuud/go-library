package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/zhuud/go-library/svc/conf"
)

var (
	// enableSlowLog 是否启用慢日志打印，默认为 true
	enableSlowLog = true
)

const (
	// defaultReaderSlowThreshold 默认慢日志阈值，超过此耗时的消息将打印慢日志
	defaultReaderSlowThreshold = time.Second * 3
	// defaultWriterSlowThreshold 默认慢日志阈值，超过此耗时的消息将打印慢日志
	defaultWriterSlowThreshold = time.Second * 2
)

type KafkaConf struct {
	Brokers  []string `json:"brokers"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	CaFile   string   `json:"ca_file"`
}

// SetEnableSlowLog 设置是否启用慢日志打印（全局开关）
// 默认为 true，设置为 false 可以关闭所有 Kafka reader 和 writer 的慢日志打印
func SetEnableSlowLog(enable bool) {
	enableSlowLog = enable
}

// EnableSlowLog 获取是否启用慢日志打印
func EnableSlowLog() bool {
	return enableSlowLog
}

// GetServers 获取 Kafka 服务器地址列表
// 优先从 qconf 配置中获取，如果不存在则从 KafkaConf 配置中获取
func GetServers() ([]string, error) {
	servers := make([]string, 0)

	var clusters map[string]struct {
		Host string `json:"host"`
	}
	_ = conf.GetUnmarshal(fmt.Sprintf("/qconf/web-config/%s", "kafka_cluster"), &clusters)
	if cluster, ok := clusters["tobase"]; ok {
		servers = strings.Split(cluster.Host, ",")
	}
	if len(servers) == 0 {
		c := KafkaConf{}
		_ = conf.GetUnmarshal("Kafka", &c)
		servers = c.Brokers
	}
	if len(servers) == 0 {
		return servers, fmt.Errorf("kafka.GetServers not set address, please set config Kafka")
	}
	return servers, nil
}

// GetSASL 获取 Kafka SASL 认证信息
// 优先从 qconf 配置中获取，如果不存在则从 KafkaConf 配置中获取
func GetSASL() (username, password string) {
	c := KafkaConf{}
	_ = conf.GetUnmarshal("Kafka", &c)
	return c.Username, c.Password
}

// GetTLS 获取 Kafka TLS CA 证书文件路径
// 优先从 qconf 配置中获取，如果不存在则从 KafkaConf 配置中获取
func GetTLS() string {
	c := KafkaConf{}
	_ = conf.GetUnmarshal("Kafka", &c)
	return c.CaFile
}
