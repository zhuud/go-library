package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/breaker"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zhuud/go-library/svc/conf"
	"github.com/zhuud/go-library/svc/kafka/internal"
	"github.com/zhuud/go-library/utils"
)

type (
	produceMessage struct {
		Topic     any    `json:"topic"`
		From      string `json:"from"`
		Timestamp int64  `json:"timestamp"`
		Data      any    `json:"data"`
		// Deprecated
		LogId string `json:"logid"`
	}
	// WriterOptionFunc 是 internal.WriterOptionFunc 的类型别名，方便外部包使用
	WriterOptionFunc = internal.WriterOptionFunc
)

var (
	producerManager          = syncx.NewResourceManager()
	producerManagerCloseOnce sync.Once
)

// Push 推送消息到 Kafka
func Push(ctx context.Context, topic string, data any, opts ...WriterOptionFunc) error {
	// 根据参数初始化 producer (第一次初始化时确定)
	producer, err := NewProducer(topic, opts...)
	if err != nil {
		return err
	}

	fileName := ""
	if _, file, line, ok := runtime.Caller(1); ok {
		fileName = fmt.Sprintf("%s:%d", file, line)
	}

	produce := &produceMessage{
		Topic:     topic,
		From:      fileName,
		Timestamp: time.Now().Unix(),
		Data:      data,
		LogId:     utils.TraceIDFromContext(ctx),
	}
	produceJson, err := json.Marshal(produce)
	if err != nil {
		logx.WithContext(ctx).Errorf("kafka.Push.Marshal data: %v, error: %v", produce, err)
		return err
	}

	return producer.Push(ctx, string(produceJson))
}

// newProducer 创建 Producer 实例
func NewProducer(topic string, opts ...WriterOptionFunc) (*internal.Writer, error) {
	// 确保关闭监听器只注册一次
	producerManagerCloseOnce.Do(func() {
		proc.AddShutdownListener(func() {
			_ = producerManager.Close()
		})
	})
	servers, err := internal.GetServers()
		if err != nil {
			return nil, err
		}
	if len(servers) == 0 {
		return nil, fmt.Errorf("kafka.NewProducer servers not set")
	}
	if len(topic) == 0 {
		return nil, fmt.Errorf("kafka.NewProducer topic not set")
	}

	resource, err := producerManager.GetResource(topic, func() (io.Closer, error) {
		if conf.IsLocal() {
			opts = append(opts, WithAllowAutoTopicCreation())
		}

		producer := internal.NewWriter(servers, topic, opts...)
		return producer, nil
	})
	if err != nil {
		return nil, err
	}

	return resource.(*internal.Writer), nil
}

// WithAllowAutoTopicCreation 允许自动创建 topic
func WithAllowAutoTopicCreation() WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.AllowAutoTopicCreation = true
	}
}

// WithBatchSize 配置批量大小（消息数量）
func WithBatchSize(size int) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.BatchSize = size
	}
}

// WithBatchBytes 配置批量大小（字节数）
func WithBatchBytes(bytes int64) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.BatchBytes = bytes
	}
}

// WithBatchTimeout 配置批次刷新超时时间
func WithBatchTimeout(timeout time.Duration) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.BatchTimeout = timeout
	}
}

// WithAsync 配置是否异步模式
func WithAsync(async bool) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.Async = &async
	}
}

// WithRequiredAcks 配置需要的确认数
func WithRequiredAcks(acks kafka.RequiredAcks) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.RequiredAcks = acks
	}
}

// WithReadTimeout 配置读取超时时间
func WithReadTimeout(timeout time.Duration) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.ReadTimeout = timeout
	}
}

// WithWriteTimeout 配置写入超时时间
func WithWriteTimeout(timeout time.Duration) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.WriteTimeout = timeout
	}
}

// WithMaxAttempts 配置最大尝试次数
func WithMaxAttempts(attempts int) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.MaxAttempts = attempts
	}
}

// WithBalancer 配置分区平衡器
func WithBalancer(balancer kafka.Balancer) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.Balancer = balancer
	}
}

// WithCompression 配置压缩算法
func WithCompression(compression kafka.Compression) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.Compression = compression
	}
}

// WithWriteBackoffMin 配置写入退避最小时间
func WithWriteBackoffMin(min time.Duration) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.WriteBackoffMin = min
	}
}

// WithWriteBackoffMax 配置写入退避最大时间
func WithWriteBackoffMax(max time.Duration) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.WriteBackoffMax = max
	}
}

// WithCompletion 配置完成回调函数
func WithCompletion(completion func(messages []kafka.Message, err error)) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.Completion = completion
	}
}

// WithBreaker 配置断路器
func WithBreaker(brk breaker.Breaker) WriterOptionFunc {
	return func(config *internal.WriterConf) {
		config.Breaker = brk
	}
}
