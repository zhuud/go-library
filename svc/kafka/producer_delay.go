package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zhuud/go-library/svc/kafka/internal"
)

// DelayOptionFunc 是 internal.DelayOptionFunc 的类型别名，方便外部包使用
type DelayOptionFunc = internal.DelayOptionFunc

// 关注错误
// kafka.delay consumer panic
// kafka.delay.sendAndAck Unmarshal data: %s, error: %v
// kafka.delay.sendAndAck.Push kafka data: %s, error: %v
// kafka.delay.sendAndAck.Ack data: %s, error: %v
// kafka.delay.FailAck max retry attempts exceeded, topic: %s, attempts: %d, max: %d

const (
	delayTrickInterval = time.Second
)

var (
	delayOnce sync.Once
	hasSet    int32
	delayer   *internal.Delayer
)

// DelaySetUp 设置延迟队列
func DelaySetUp(redis *redis.Redis, prefix string, opts ...DelayOptionFunc) {
	delayOnce.Do(func() {
		// 将 prefix 配置添加到 opts 最前面，确保不会被后续 opts 覆盖
		opts = append([]DelayOptionFunc{WithDelayPrefix(prefix)}, opts...)
		delayer = internal.NewDelayer(redis, opts...)

		// 注册 WrapUp 监听器，确保优雅关闭
		proc.AddWrapUpListener(func() {
			delayer.Stop()
			log.Println("kafka.delay consumer stopped")
		})

		// 启动后台消费，消息处理逻辑已完全封装在 Delayer 内部
		// handler 负责将消息发送到 Kafka，返回 error 表示失败将触发重试
		delayer.Start(func(ctx context.Context, delayData *internal.DelayData) error {
			return Push(ctx, delayData.Topic, delayData.Data)
		}, delayTrickInterval)

		atomic.AddInt32(&hasSet, 1)
	})
}

func PushDelay(ctx context.Context, topic string, data any, delayDuration time.Duration) error {
	if atomic.LoadInt32(&hasSet) == 0 {
		return fmt.Errorf("kafka.PushDelay must DelaySetUp before PushDelay")
	}
	return delayer.Push(ctx, topic, data, delayDuration)
}

// WithDelayPrefix 配置队列 redis 名称前缀
func WithDelayPrefix(prefix string) DelayOptionFunc {
	return func(config *internal.DelayConf) {
		config.Prefix = prefix
	}
}

// WithDelayBatchSize 配置从队列中获取数据的批量大小
func WithDelayBatchSize(size int) DelayOptionFunc {
	return func(config *internal.DelayConf) {
		if size > 0 {
			config.BatchSize = size
		}
	}
}

// WithDelayMaxRetryAttempts 配置最大重试次数
func WithDelayMaxRetryAttempts(attempts int) DelayOptionFunc {
	return func(config *internal.DelayConf) {
		if attempts > 0 {
			config.MaxRetryAttempts = attempts
		}
	}
}

// WithDelayRetryDelay 配置重试延迟时间
func WithDelayRetryDelay(delay time.Duration) DelayOptionFunc {
	return func(config *internal.DelayConf) {
		if delay > 0 {
			config.RetryDelayDuration = delay
		}
	}
}

// WithDelayConcurrency 配置并发处理协程数
func WithDelayConcurrency(concurrency int) DelayOptionFunc {
	return func(config *internal.DelayConf) {
		if concurrency > 0 {
			config.Concurrency = concurrency
		}
	}
}
