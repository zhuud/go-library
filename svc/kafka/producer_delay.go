package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zhuud/go-library/svc/delay"
)

// Kafka 层选项别名，保持调用方 import kafka 即可使用
var (
	WithDelayMaxPushDelay     = delay.WithMaxPushDelay
	WithDelayConsumeInterval  = delay.WithConsumeInterval
	WithDelayBatchSize        = delay.WithBatchSize
	WithDelayConcurrency      = delay.WithConcurrency
	WithDelayHandlerTimeout   = delay.WithHandlerTimeout
	WithDelayMaxRetryAttempts = delay.WithMaxRetryAttempts
	WithDelayRetryDelay       = delay.WithRetryDelay
	WithDelayReservedTimeout  = delay.WithReservedTimeout
)

// DelayOptionFunc 是 delay.OptionFunc 的类型别名，方便外部包使用
type DelayOptionFunc = delay.OptionFunc

const delayPrefix = "kafka"

var (
	delayOnce     sync.Once
	delayInstance *delay.Delay
)

// DelaySetUp 创建 Kafka 延迟队列并启动消费（全局单例 + 自动注册关闭钩子）。
// 幂等安全，多次调用只有首次生效。
func DelaySetUp(redis *redis.Redis, opts ...DelayOptionFunc) {
	delayOnce.Do(func() {
		dl := delay.NewDelay(redis, delayPrefix, opts...)

		dl.Start(func(ctx context.Context, msg *delay.Message) error {
			return Push(ctx, msg.Key, msg.Data)
		})

		proc.AddWrapUpListener(func() {
			dl.Stop()
			log.Printf("kafka.delayer consumer closed, prefix:%s", delayPrefix)
		})

		delayInstance = dl
	})
}

// PushDelay 推送延迟消息到全局 Kafka 延迟队列。
// 必须先调用 DelaySetUp 完成初始化。
func PushDelay(ctx context.Context, topic string, data any, delayDuration time.Duration) error {
	if delayInstance == nil {
		return fmt.Errorf("kafka.delayer not set up, call DelaySetUp first")
	}
	return delayInstance.Push(ctx, topic, data, delayDuration)
}