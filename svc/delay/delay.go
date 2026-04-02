package delay

import (
	"context"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zhuud/go-library/svc/delay/internal"
)

// 选项函数别名
var (
	WithPrefix           = internal.WithPrefix
	WithMaxPushDelay     = internal.WithMaxPushDelay
	WithConsumeInterval  = internal.WithConsumeInterval
	WithBatchSize        = internal.WithBatchSize
	WithConcurrency      = internal.WithConcurrency
	WithHandlerTimeout   = internal.WithHandlerTimeout
	WithMaxRetryAttempts = internal.WithMaxRetryAttempts
	WithRetryDelay       = internal.WithRetryDelay
	WithReservedTimeout  = internal.WithReservedTimeout
)

// 类型别名，公开 internal 中的核心类型
type (
	Message        = internal.Message
	MessageHandler = internal.MessageHandler
	OptionFunc     = internal.OptionFunc
	Config         = internal.Config
)


var (
	mu       sync.Mutex
	delayers sync.Map
)

// Delay 延迟队列实例
type Delay struct {
	delayer *internal.Delayer
}

// NewDelay 创建延迟队列实例（per-prefix 单例，相同 prefix 返回已有实例）
func NewDelay(redisClient *redis.Redis, prefix string, opts ...OptionFunc) *Delay {
	if v, ok := delayers.Load(prefix); ok {
		return v.(*Delay)
	}

	mu.Lock()
	defer mu.Unlock()

	if v, ok := delayers.Load(prefix); ok {
		return v.(*Delay)
	}

	opts = append(opts, internal.WithPrefix(prefix))
	d := &Delay{delayer: internal.NewDelayer(redisClient, opts...)}
	delayers.Store(prefix, d)

	return d
}

// Start 启动后台消费
func (d *Delay) Start(handler MessageHandler) {
	d.delayer.Start(handler)
}

// Stop 停止消费并等待退出
func (d *Delay) Stop() {
	d.delayer.Stop()
}

// Push 推送延迟消息
func (d *Delay) Push(ctx context.Context, key string, data any, delayDuration time.Duration) error {
	return d.delayer.Push(ctx, key, data, delayDuration)
}
