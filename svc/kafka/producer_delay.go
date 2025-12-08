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
	once    sync.Once
	hasSet  int32
	delayer *internal.Delayer
)

func DelaySetUp(redis *redis.Redis, batchSize int, prefix string) {
	DelaySetUpWithRetry(redis, batchSize, prefix, 0, 0)
}

// DelaySetUpWithRetry 设置延迟队列，支持配置重试参数
// maxAttempts: 最大重试次数，0 表示使用默认值 3
// retryDelay: 重试延迟时间，0 表示使用默认值 1 分钟
func DelaySetUpWithRetry(redis *redis.Redis, batchSize int, prefix string, maxAttempts int, retryDelay time.Duration) {
	once.Do(func() {
		opts := []internal.DelayOptionFunc{
			internal.WithDelayPrefix(prefix),
		}
		if batchSize > 0 {
			opts = append(opts, internal.WithDelayBatchSize(batchSize))
		}
		if maxAttempts > 0 {
			opts = append(opts, internal.WithDelayMaxRetryAttempts(maxAttempts))
		}
		if retryDelay > 0 {
			opts = append(opts, internal.WithDelayRetryDelay(retryDelay))
		}
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
