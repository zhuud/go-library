package internal

import (
	"context"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/breaker"
	"github.com/zeromicro/go-zero/core/stat"
	"go.opentelemetry.io/otel"
)

var (
	// defaultWriterBalancer 默认分区平衡器
	defaultWriterBalancer = &kafka.LeastBytes{}
)

const (
	// defaultWriterCompression 默认压缩算法
	defaultWriterCompression = kafka.Snappy
	// defaultWriterRequiredAcks 默认需要的确认数，仅等待 leader 副本写入本地磁盘并返回确认
	defaultWriterRequiredAcks = kafka.RequireOne
	// defaultWriterBatchSize 默认批量大小限制（消息数量）
	defaultWriterBatchSize = 100
	// defaultWriterBatchBytes 默认批量大小限制（字节数），1MB
	defaultWriterBatchBytes = 1 * 1024 * 1024
	// defaultWriterBatchTimeout 默认不完整消息批次的刷新时间限制，50毫秒
	defaultWriterBatchTimeout = 50 * time.Millisecond
	// defaultWriterAsync 默认是否异步模式
	defaultWriterAsync = true
)

// Kafka Writer
// 支持参数配置
// 支持日志记录、慢日志记录
// 支持熔断器
// 支持 metrics 统计
// 支持 otel 追踪
type (
	kafkaWriter interface {
		Close() error
		WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	}

	WriterOptionFunc func(config *WriterConf)

	// WriterConf Writer 配置结构体
	WriterConf struct {
		// 基础配置
		AllowAutoTopicCreation bool // 是否允许自动创建 topic，默认 false

		// 性能配置
		BatchSize    int                // 批量大小限制（消息数量），默认 100（kafka-go 默认 100）
		BatchBytes   int64              // 批量大小限制（字节数），默认 1M（kafka-go 默认 1M）
		BatchTimeout time.Duration      // 不完整消息批次的刷新时间限制，默认 50 毫秒（kafka-go 默认 1 秒）
		Async        *bool              // 是否异步模式，默认 true（kafka-go 默认 false）
		RequiredAcks kafka.RequiredAcks // 需要的确认数，默认 RequireOne (1)（kafka-go 默认 RequireNone (0)）

		// 网络配置
		ReadTimeout  time.Duration // 读取操作的超时时间（kafka-go 默认 10 秒）
		WriteTimeout time.Duration // 写入操作的超时时间（kafka-go 默认 10 秒）
		MaxAttempts  int           // 消息传递的最大尝试次数（kafka-go 默认 10）

		// 消息配置
		Balancer    kafka.Balancer    // 分区平衡器，默认 LeastBytes（kafka-go 默认 RoundRobin）
		Compression kafka.Compression // 压缩算法，默认 Snappy（kafka-go 默认 CompressionNone）

		// 写入配置
		WriteBackoffMin time.Duration // 写入退避最小时间（kafka-go 默认 100ms）
		WriteBackoffMax time.Duration // 写入退避最大时间（kafka-go 默认 1 秒）

		// 回调配置
		Completion func(messages []kafka.Message, err error) // 完成回调函数

		// 断路器配置
		Breaker breaker.Breaker // 断路器（nil 时不启用熔断）
	}

	Writer struct {
		topic       string
		writer      kafkaWriter
		metrics     *stat.Metrics
		infoLogger  *writerLogger
		errorLogger *writerErrorLogger
		breaker     breaker.Breaker // 断路器（可选），如果为 nil 则不启用熔断
	}
)

// NewWriter 创建 Writer 实例
func NewWriter(addrs []string, topic string, opts ...WriterOptionFunc) *Writer {
	// 创建 logger 实例
	infoLogger := newWriterLogger(topic)
	errorLogger := newWriterErrorLogger(topic)
	metrics := stat.NewMetrics("kafka.writer." + topic)

	var config WriterConf
	for _, opt := range opts {
		opt(&config)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(addrs...),
		Topic:        topic,
		BatchSize:    defaultWriterBatchSize,
		BatchBytes:   defaultWriterBatchBytes,
		BatchTimeout: defaultWriterBatchTimeout,
		Async:        defaultWriterAsync,
		Completion:   newDefaultCompletionCallback(infoLogger, errorLogger, metrics),
		RequiredAcks: defaultWriterRequiredAcks,
		Balancer:     defaultWriterBalancer,
		Compression:  defaultWriterCompression,
		Logger:       infoLogger,
		ErrorLogger:  errorLogger,
	}

	// 应用配置（只设置显式配置的选项）
	// 基础配置
	writer.AllowAutoTopicCreation = config.AllowAutoTopicCreation

	// 性能配置
	if config.BatchSize > 0 {
		writer.BatchSize = config.BatchSize
	}
	if config.BatchBytes > 0 {
		writer.BatchBytes = config.BatchBytes
	}
	if config.BatchTimeout > 0 {
		writer.BatchTimeout = config.BatchTimeout
	}
	if config.Async != nil {
		writer.Async = *config.Async
	}
	if config.RequiredAcks != 0 {
		writer.RequiredAcks = config.RequiredAcks
	}

	// 网络配置
	if config.ReadTimeout > 0 {
		writer.ReadTimeout = config.ReadTimeout
	}
	if config.WriteTimeout > 0 {
		writer.WriteTimeout = config.WriteTimeout
	}
	if config.MaxAttempts > 0 {
		writer.MaxAttempts = config.MaxAttempts
	}

	// 消息配置
	if config.Balancer != nil {
		writer.Balancer = config.Balancer
	}
	if config.Compression != 0 {
		writer.Compression = config.Compression
	}

	// 写入配置
	if config.WriteBackoffMin > 0 {
		writer.WriteBackoffMin = config.WriteBackoffMin
	}
	if config.WriteBackoffMax > 0 {
		writer.WriteBackoffMax = config.WriteBackoffMax
	}

	// 回调配置
	// 只有当 completion 被显式设置时才覆盖默认值
	if config.Completion != nil {
		writer.Completion = config.Completion
	}

	return &Writer{
		topic:       topic,
		writer:      writer,
		metrics:     metrics,
		infoLogger:  infoLogger,
		errorLogger: errorLogger,
		breaker:     config.Breaker, // 使用配置中的 breaker，如果为 nil 则不启用熔断
	}
}

// Close 关闭 Writer 并释放资源
func (w *Writer) Close() error {
	return w.writer.Close()
}

// Name 返回 Writer 发送消息的 topic 名称
func (w *Writer) Name() string {
	return w.topic
}

// Push 发送消息到 Kafka topic
func (w *Writer) Push(ctx context.Context, v string) error {
	return w.PushWithKey(ctx, strconv.FormatInt(time.Now().UnixNano(), 10), v)
}

// PushWithKey 发送带 key 的消息到 Kafka topic
func (w *Writer) PushWithKey(ctx context.Context, key, v string) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(v),
	}

	// wrap message into message carrier
	mc := NewMessageCarrier(NewMessage(&msg))
	// inject trace context into message
	otel.GetTextMapPropagator().Inject(ctx, mc)

	// 如果配置了断路器，使用断路器包装请求执行
	if w.breaker != nil {
		return w.breaker.DoCtx(ctx, func() error {
			return w.writer.WriteMessages(ctx, msg)
		})
	}

	// 如果没有配置断路器，直接执行请求
	return w.writer.WriteMessages(ctx, msg)
}
