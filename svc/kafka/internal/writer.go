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

	PushOption func(options *pushOptions)

	pushOptions struct {
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
func NewWriter(addrs []string, topic string, opts ...PushOption) *Writer {
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

	var options pushOptions
	for _, opt := range opts {
		opt(&options)
	}

	producer.AllowAutoTopicCreation = options.allowAutoTopicCreation
	producer.Async = options.async

	// apply kafka.Writer options (only set if explicitly configured)
	if options.balancer != nil {
		producer.Balancer = options.balancer
	}
	if options.batchSize > 0 {
		producer.BatchSize = options.batchSize
	}
	if options.batchBytes > 0 {
		producer.BatchBytes = options.batchBytes
	}
	if options.batchTimeout > 0 {
		producer.BatchTimeout = options.batchTimeout
	}
	if options.requiredAcks != 0 {
		producer.RequiredAcks = options.requiredAcks
	}
	if options.readTimeout > 0 {
		producer.ReadTimeout = options.readTimeout
	}
	if options.writeTimeout > 0 {
		producer.WriteTimeout = options.writeTimeout
	}
	if options.maxAttempts > 0 {
		producer.MaxAttempts = options.maxAttempts
	}
	if options.compression != 0 {
		producer.Compression = options.compression
	}

	// 只有当 completion 被显式设置时才覆盖默认值
	if options.completion != nil {
		producer.Completion = options.completion
	}

	return &Writer{
		producer: producer,
		topic:    topic,
		metrics:  metrics,
		breaker:  options.breaker, // 使用配置中的 breaker，如果为 nil 则不启用熔断
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
func WithAllowAutoTopicCreation() PushOption {
	return func(options *pushOptions) {
		options.allowAutoTopicCreation = true
	}
}

// WithBalancer customizes the Writer with the given balancer.
func WithBalancer(balancer kafka.Balancer) PushOption {
	return func(options *pushOptions) {
		options.balancer = balancer
	}
}

// WithBatchSize customizes the Writer with the given batch size.
func WithBatchSize(size int) PushOption {
	return func(options *pushOptions) {
		options.batchSize = size
	}
}

// WithBatchTimeout customizes the Writer with the given batch timeout.
func WithBatchTimeout(timeout time.Duration) PushOption {
	return func(options *pushOptions) {
		options.batchTimeout = timeout
	}
}

// WithBatchBytes customizes the Writer with the given batch bytes.
func WithBatchBytes(bytes int64) PushOption {
	return func(options *pushOptions) {
		options.batchBytes = bytes
	}
}

// WithAsync enables async push mode (disables synchronous push).
func WithAsync(async bool) PushOption {
	return func(options *pushOptions) {
		options.async = async
	}
}

// WithRequiredAcks customizes the Writer with the given required acks.
func WithRequiredAcks(acks kafka.RequiredAcks) PushOption {
	return func(options *pushOptions) {
		options.requiredAcks = acks
	}
}

// WithReadTimeout customizes the Writer with the given read timeout.
func WithReadTimeout(timeout time.Duration) PushOption {
	return func(options *pushOptions) {
		options.readTimeout = timeout
	}
}

// WithWriteTimeout customizes the Writer with the given write timeout.
func WithWriteTimeout(timeout time.Duration) PushOption {
	return func(options *pushOptions) {
		options.writeTimeout = timeout
	}
}

// WithMaxAttempts customizes the Writer with the given max attempts.
func WithMaxAttempts(attempts int) PushOption {
	return func(options *pushOptions) {
		options.maxAttempts = attempts
	}
}

// WithCompression customizes the Writer with the given compression.
func WithCompression(compression kafka.Compression) PushOption {
	return func(options *pushOptions) {
		options.compression = compression
	}
}

// WithCompletion customizes the Writer with the given completion callback.
// This callback is called when messages are successfully delivered or failed.
// If async mode is enabled and no completion callback is provided, a default
// callback that logs with logx will be used.
func WithCompletion(completion func(messages []kafka.Message, err error)) PushOption {
	return func(options *pushOptions) {
		options.completion = completion
	}
}

// WithBreaker 设置断路器（可选）
// 如果不设置，则不启用熔断功能
func WithBreaker(brk breaker.Breaker) PushOption {
	return func(options *pushOptions) {
		options.breaker = brk
	}
}
