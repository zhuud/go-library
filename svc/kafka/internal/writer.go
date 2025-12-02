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

	PushOptionFunc func(config *pushConf)

	pushConf struct {
		// kafka.Writer options
		allowAutoTopicCreation bool
		balancer               kafka.Balancer
		batchSize              int
		batchBytes             int64
		batchTimeout           time.Duration
		async                  bool
		completion             func(messages []kafka.Message, err error)
		requiredAcks           kafka.RequiredAcks
		readTimeout            time.Duration
		writeTimeout           time.Duration
		maxAttempts            int
		compression            kafka.Compression

		// Breaker 断路器（可选），如果为 nil 则不启用熔断
		breaker breaker.Breaker
	}

	Writer struct {
		topic    string
		producer kafkaWriter
		metrics  *stat.Metrics
		breaker  breaker.Breaker // 断路器（可选），如果为 nil 则不启用熔断
	}
)

// NewWriter returns a Writer with the given Kafka addresses and topic.
func NewWriter(addrs []string, topic string, opts ...PushOptionFunc) *Writer {
	// 创建 logger 实例
	infoLogger := newWriterLogger(topic)
	errorLogger := newWriterErrorLogger(topic)
	metrics := stat.NewMetrics("kafka.writer." + topic)

	producer := &kafka.Writer{
		Addr:         kafka.TCP(addrs...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		Compression:  kafka.Snappy,
		Completion:   newDefaultCompletionCallback(infoLogger, errorLogger, metrics), // 默认 completion callback
		RequiredAcks: kafka.RequireOne,                                               // 默认 仅等待 leader 副本写入本地磁盘并返回确认，不关心 follower 是否同步
		Logger:       infoLogger,
		ErrorLogger:  errorLogger,
	}

	var config pushConf
	for _, opt := range opts {
		opt(&config)
	}

	producer.AllowAutoTopicCreation = config.allowAutoTopicCreation
	producer.Async = config.async

	// apply kafka.Writer options (only set if explicitly configured)
	if config.balancer != nil {
		producer.Balancer = config.balancer
	}
	if config.batchSize > 0 {
		producer.BatchSize = config.batchSize
	}
	if config.batchBytes > 0 {
		producer.BatchBytes = config.batchBytes
	}
	if config.batchTimeout > 0 {
		producer.BatchTimeout = config.batchTimeout
	}
	if config.requiredAcks != 0 {
		producer.RequiredAcks = config.requiredAcks
	}
	if config.readTimeout > 0 {
		producer.ReadTimeout = config.readTimeout
	}
	if config.writeTimeout > 0 {
		producer.WriteTimeout = config.writeTimeout
	}
	if config.maxAttempts > 0 {
		producer.MaxAttempts = config.maxAttempts
	}
	if config.compression != 0 {
		producer.Compression = config.compression
	}

	// 只有当 completion 被显式设置时才覆盖默认值
	if config.completion != nil {
		producer.Completion = config.completion
	}

	return &Writer{
		producer: producer,
		topic:    topic,
		metrics:  metrics,
		breaker:  config.breaker, // 使用配置中的 breaker，如果为 nil 则不启用熔断
	}
}

// Close closes the Writer and releases any resources used by it.
func (w *Writer) Close() error {
	return w.producer.Close()
}

// Name returns the name of the Kafka topic that the Writer is sending messages to.
func (w *Writer) Name() string {
	return w.topic
}

// Push sends a message to the Kafka topic.
func (w *Writer) Push(ctx context.Context, v string) error {
	return w.PushWithKey(ctx, strconv.FormatInt(time.Now().UnixNano(), 10), v)
}

// PushWithKey sends a message with the given key to the Kafka topic.
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
			return w.producer.WriteMessages(ctx, msg)
		})
	}

	// 如果没有配置断路器，直接执行请求
	return w.producer.WriteMessages(ctx, msg)
}

// WithAllowAutoTopicCreation allows the Writer to create the given topic if it does not exist.
func WithAllowAutoTopicCreation() PushOptionFunc {
	return func(config *pushConf) {
		config.allowAutoTopicCreation = true
	}
}

// WithBalancer customizes the Writer with the given balancer.
func WithBalancer(balancer kafka.Balancer) PushOptionFunc {
	return func(config *pushConf) {
		config.balancer = balancer
	}
}

// WithBatchSize customizes the Writer with the given batch size.
func WithBatchSize(size int) PushOptionFunc {
	return func(config *pushConf) {
		config.batchSize = size
	}
}

// WithBatchTimeout customizes the Writer with the given batch timeout.
func WithBatchTimeout(timeout time.Duration) PushOptionFunc {
	return func(config *pushConf) {
		config.batchTimeout = timeout
	}
}

// WithBatchBytes customizes the Writer with the given batch bytes.
func WithBatchBytes(bytes int64) PushOptionFunc {
	return func(config *pushConf) {
		config.batchBytes = bytes
	}
}

// WithAsync enables async push mode (disables synchronous push).
func WithAsync(async bool) PushOptionFunc {
	return func(config *pushConf) {
		config.async = async
	}
}

// WithRequiredAcks customizes the Writer with the given required acks.
func WithRequiredAcks(acks kafka.RequiredAcks) PushOptionFunc {
	return func(config *pushConf) {
		config.requiredAcks = acks
	}
}

// WithReadTimeout customizes the Writer with the given read timeout.
func WithReadTimeout(timeout time.Duration) PushOptionFunc {
	return func(config *pushConf) {
		config.readTimeout = timeout
	}
}

// WithWriteTimeout customizes the Writer with the given write timeout.
func WithWriteTimeout(timeout time.Duration) PushOptionFunc {
	return func(config *pushConf) {
		config.writeTimeout = timeout
	}
}

// WithMaxAttempts customizes the Writer with the given max attempts.
func WithMaxAttempts(attempts int) PushOptionFunc {
	return func(config *pushConf) {
		config.maxAttempts = attempts
	}
}

// WithCompression customizes the Writer with the given compression.
func WithCompression(compression kafka.Compression) PushOptionFunc {
	return func(config *pushConf) {
		config.compression = compression
	}
}

// WithCompletion customizes the Writer with the given completion callback.
// This callback is called when messages are successfully delivered or failed.
// If async mode is enabled and no completion callback is provided, a default
// callback that logs with logx will be used.
func WithCompletion(completion func(messages []kafka.Message, err error)) PushOptionFunc {
	return func(config *pushConf) {
		config.completion = completion
	}
}

// WithBreaker 设置断路器（可选）
// 如果不设置，则不启用熔断功能
func WithBreaker(brk breaker.Breaker) PushOptionFunc {
	return func(config *pushConf) {
		config.breaker = brk
	}
}
