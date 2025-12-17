package kafka

import (
	"github.com/zhuud/go-library/svc/kafka/internal"
)

type KafkaConf = internal.KafkaConf

// SetEnableSlowLog 设置是否启用慢日志打印（全局开关）
// 默认为 true，设置为 false 可以关闭所有 Kafka reader 和 writer 的慢日志打印
func SetEnableSlowLog(enable bool) {
	internal.SetEnableSlowLog(enable)
}
