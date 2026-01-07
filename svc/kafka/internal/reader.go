package internal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zhuud/go-library/utils"
	"go.opentelemetry.io/otel"
)

const (
	// defaultMaxWait 默认等待新数据的最大时间
	defaultMaxWait = 100 * time.Millisecond
	// defaultCommitInterval 默认提交偏移量的间隔
	defaultCommitInterval = 400 * time.Millisecond
	// defaultQueueCapacity 默认内部消息队列容量
	defaultQueueCapacity = 1000
	// defaultStartOffset 默认起始 offset，从最新开始
	defaultStartOffset = kafka.LastOffset
	// defaultWatchPartitionChanges 默认是否监听分区变化
	defaultWatchPartitionChanges = true
	// defaultMinBytes 默认最小字节数，128字节
	defaultMinBytes = 128
	// defaultMaxBytes 默认最大字节数，4M
	defaultMaxBytes = 4 * 1024 * 1024
	// defaultProcessors 默认处理协程数量
	defaultProcessors = 8
	// defaultConsumers 默认消费协程数量
	defaultConsumers = 8
)

// Kafka Reader
// 支持参数配置
// 支持日志记录、慢日志记录
// 支持 metrics 统计
// 支持 otel 追踪
type (
	kafkaReader interface {
		Close() error
		FetchMessage(ctx context.Context) (kafka.Message, error)
		CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	}

	ReaderOptionFunc func(config *ReaderConf)

	// ReaderConf Reader 配置结构体
	ReaderConf struct {
		// 消费配置
		Processors int // 处理协程数量，默认 8
		Consumers  int // 消费协程数量，默认 8

		// 认证配置
		Username string // SASL 用户名
		Password string // SASL 密码
		CaFile   string // TLS CA 证书文件路径

		// 基础配置
		StartOffset int64 // 起始 offset，FirstOffset(-2) 或 LastOffset(-1)，默认 LastOffset(-1)

		// 性能配置
		MinBytes       int           // 最小字节数，默认 10K（kafka-go 默认 1）
		MaxBytes       int           // 最大字节数，默认 4M（kafka-go 默认 1M）
		MaxWait        time.Duration // 等待新数据的最大时间，默认 50ms（kafka-go 默认 10 秒）
		CommitInterval time.Duration // 提交偏移量的间隔，默认 200ms（kafka-go 默认 0，同步提交）
		QueueCapacity  int           // 内部消息队列容量，默认 1000（kafka-go 默认 100）

		// 网络配置
		ReadBatchTimeout time.Duration // 从批次中获取消息的超时时间（kafka-go 默认 10 秒）
		SessionTimeout   time.Duration // 会话超时（kafka-go 默认 30 秒）
		MaxAttempts      int           // 连接尝试的最大次数（kafka-go 默认3次）

		// 消费组配置
		HeartbeatInterval      time.Duration         // 心跳间隔（kafka-go 默认 3 秒）
		RebalanceTimeout       time.Duration         // 重平衡超时（kafka-go 默认 30 秒）
		JoinGroupBackoff       time.Duration         // 加入组退避时间（kafka-go 默认 5 秒）
		PartitionWatchInterval time.Duration         // 分区监听间隔（kafka-go 默认 5 秒）
		WatchPartitionChanges  *bool                 // 是否监听分区变化，默认 true（kafka-go 默认 false）
		GroupBalancers         []kafka.GroupBalancer // 消费组平衡器（kafka-go 默认 [Range, RoundRobin]）

		// 读取配置
		ReadBackoffMin time.Duration // 读取退避最小时间（kafka-go 默认 100ms）
		ReadBackoffMax time.Duration // 读取退避最大时间（kafka-go 默认 1 秒）
	}

	// ConsumeHandler 消费消息的处理器接口
	ConsumeHandler interface {
		Consume(ctx context.Context, key, value string) error
	}

	Reader struct {
		topic            string
		group            string
		handler          ConsumeHandler
		reader           kafkaReader
		flowMetrics      *stat.Metrics // 流转耗时
		bizMetrics       *stat.Metrics // 业务处理耗时
		infoLogger       *readerLogger
		errorLogger      *readerErrorLogger
		channel          chan kafka.Message
		fetcherRoutines  *threading.RoutineGroup
		consumerRoutines *threading.RoutineGroup
		processors       int // 处理协程数量
		consumers        int // 消费协程数量
	}
)

// NewReader 创建 Reader 实例
func NewReader(brokers []string, topic, group string, handler ConsumeHandler, opts ...ReaderOptionFunc) *Reader {
	// 创建 logger 实例
	infoLogger := newReaderLogger(group)
	errorLogger := newReaderErrorLogger(group)

	var config ReaderConf
	for _, opt := range opts {
		opt(&config)
	}

	// 构建 kafka.ReaderConfig
	readerConfig := kafka.ReaderConfig{
		Brokers:               brokers,
		GroupID:               group,
		Topic:                 topic,
		StartOffset:           defaultStartOffset,
		MinBytes:              defaultMinBytes,
		MaxBytes:              defaultMaxBytes,
		MaxWait:               defaultMaxWait,
		CommitInterval:        defaultCommitInterval,
		QueueCapacity:         defaultQueueCapacity,
		WatchPartitionChanges: defaultWatchPartitionChanges,
		Logger:                infoLogger,
		ErrorLogger:           errorLogger,
	}

	// 应用配置（只设置显式配置的选项）
	// 基础配置
	if config.StartOffset != 0 {
		readerConfig.StartOffset = config.StartOffset
	}

	// 性能配置
	if config.MinBytes > 0 {
		readerConfig.MinBytes = config.MinBytes
	}
	if config.MaxBytes > 0 {
		readerConfig.MaxBytes = config.MaxBytes
	}
	if config.MaxWait > 0 {
		readerConfig.MaxWait = config.MaxWait
	}
	if config.CommitInterval > 0 {
		readerConfig.CommitInterval = config.CommitInterval
	}
	if config.QueueCapacity > 0 {
		readerConfig.QueueCapacity = config.QueueCapacity
	}

	// 网络配置
	if config.ReadBatchTimeout > 0 {
		readerConfig.ReadBatchTimeout = config.ReadBatchTimeout
	}
	if config.SessionTimeout > 0 {
		readerConfig.SessionTimeout = config.SessionTimeout
	}
	if config.MaxAttempts > 0 {
		readerConfig.MaxAttempts = config.MaxAttempts
	}

	// 消费组配置
	if config.HeartbeatInterval > 0 {
		readerConfig.HeartbeatInterval = config.HeartbeatInterval
	}
	if config.RebalanceTimeout > 0 {
		readerConfig.RebalanceTimeout = config.RebalanceTimeout
	}
	if config.JoinGroupBackoff > 0 {
		readerConfig.JoinGroupBackoff = config.JoinGroupBackoff
	}
	if config.PartitionWatchInterval > 0 {
		readerConfig.PartitionWatchInterval = config.PartitionWatchInterval
	}
	if config.WatchPartitionChanges != nil {
		readerConfig.WatchPartitionChanges = *config.WatchPartitionChanges
	}
	if len(config.GroupBalancers) > 0 {
		readerConfig.GroupBalancers = config.GroupBalancers
	}

	// 读取配置
	if config.ReadBackoffMin > 0 {
		readerConfig.ReadBackoffMin = config.ReadBackoffMin
	}
	if config.ReadBackoffMax > 0 {
		readerConfig.ReadBackoffMax = config.ReadBackoffMax
	}

	// 处理 SASL 认证
	if len(config.Username) > 0 && len(config.Password) > 0 {
		readerConfig.Dialer = &kafka.Dialer{
			SASLMechanism: plain.Mechanism{
				Username: config.Username,
				Password: config.Password,
			},
		}
	}

	// 处理 TLS 配置
	if len(config.CaFile) > 0 {
		caCert, err := os.ReadFile(config.CaFile)
		if err != nil {
			log.Fatalf("kafka.reader %s failed to read CA file: %v", group, err)
		}
		caCertPool := x509.NewCertPool()
		ok := caCertPool.AppendCertsFromPEM(caCert)
		if !ok {
			log.Fatalf("kafka.reader %s failed to parse CA certificate: %v", group, err)
		}
		if readerConfig.Dialer == nil {
			readerConfig.Dialer = &kafka.Dialer{}
		}
		readerConfig.Dialer.TLS = &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}
	}

	reader := kafka.NewReader(readerConfig)

	// 设置默认值
	if config.Processors <= 0 {
		config.Processors = defaultProcessors
	}
	if config.Consumers <= 0 {
		config.Consumers = defaultConsumers
	}

	return &Reader{
		topic:            topic,
		group:            group,
		handler:          handler,
		reader:           reader,
		flowMetrics:      stat.NewMetrics("kafka.reader.flow." + group),
		bizMetrics:       stat.NewMetrics("kafka.reader.biz." + group),
		infoLogger:       infoLogger,
		errorLogger:      errorLogger,
		channel:          make(chan kafka.Message),
		fetcherRoutines:  threading.NewRoutineGroup(),
		consumerRoutines: threading.NewRoutineGroup(),
		processors:       config.Processors,
		consumers:        config.Consumers,
	}
}

// Close 关闭 Reader 并释放资源
func (r *Reader) Close() error {
	return r.reader.Close()
}

// Start 启动消费者和处理协程
func (r *Reader) Start() {
	r.startConsumers()
	r.startFetchers()
	r.fetcherRoutines.Wait()
	close(r.channel)
	r.consumerRoutines.Wait()

	r.infoLogger.Printf("kafka.reader consumer %s closed", r.group)
}

// Stop 停止 Reader 并释放资源
func (r *Reader) Stop() {
	if err := r.reader.Close(); err != nil {
		r.errorLogger.Printf("kafka.reader close error: %v", err)
	}
}

// startConsumers 启动消费协程处理消息
func (r *Reader) startConsumers() {
	for i := 0; i < r.consumers; i++ {
		r.consumerRoutines.Run(func() {
			for msg := range r.channel {
				// wrap message into message carrier
				mc := NewMessageCarrier(NewMessage(&msg))
				// extract trace context from message
				ctx := otel.GetTextMapPropagator().Extract(context.Background(), mc)
				// remove deadline and cancel to isolate consumer processing timeout/cancel semantics
				ctx = utils.StripContext(ctx)

				if err := r.consumeOne(ctx, msg); err != nil {
					r.errorLogger.Printf("kafka.reader.consumeOne failed: topic=%s, partition=%d, offset=%d, key=%s, message=%s, error=%v",
						msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value), err)
				}

				if err := r.reader.CommitMessages(ctx, msg); err != nil {
					r.errorLogger.Printf("kafka.reader.CommitMessages failed: topic=%s, partition=%d, offset=%d, key=%s, message=%s, error=%v",
						msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value), err)
				}
			}
		})
	}
}

// consumeOne 处理单条消息
func (r *Reader) consumeOne(ctx context.Context, msg kafka.Message) (err error) {
	now := time.Now()

	// defer recover 处理 panic
	defer func() {
		if p := recover(); p != nil {
			// 获取堆栈信息
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			stackTrace := string(buf[:n])

			// 构造 panic 错误
			err = fmt.Errorf("kafka.reader.consumeOne panic: %v\n%s", p, stackTrace)

			// 记录 panic 日志
			r.errorLogger.Printf("kafka.reader.consumeOne panic: topic=%s, partition=%d, offset=%d, key=%s, message=%s, panic=%v\n%s",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value), p, stackTrace)
		}
		// 统一记录 metrics（panic 时 err != nil，正常错误时 err != nil，都视为失败）
		duration := time.Since(now)
		r.bizMetrics.Add(stat.Task{
			Duration: duration,
			Drop:     err != nil,
		})
	}()

	// 流转耗时metrics
	if duration := calculateDurationFromKey(msg.Key, now); duration >= 0 {
		r.flowMetrics.Add(stat.Task{
			Duration: duration,
		})
		// 如果超过慢日志阈值，打印慢日志
		if EnableSlowLog() && duration > defaultReaderSlowThreshold {
			r.infoLogger.Slowf("kafka.reader.flow slow: push_ns=%s, now_ms=%d, duration_ms=%d, topic=%s, partition=%d, offset=%d, group=%s, key=%s, message=%s",
				string(msg.Key), now.UnixMilli(), duration.Milliseconds(), r.topic, msg.Partition, msg.Offset, r.group, string(msg.Key), string(msg.Value))
		}
	}

	// 业务逻辑
	err = r.handler.Consume(ctx, string(msg.Key), string(msg.Value))

	return err
}

// startFetchers 启动获取协程从 Kafka 获取消息
func (r *Reader) startFetchers() {
	for i := 0; i < r.processors; i++ {
		i := i
		r.fetcherRoutines.Run(func() {
			if err := r.fetchLoop(func(msg kafka.Message) {
				r.channel <- msg
			}); err != nil {
				r.infoLogger.Printf("kafka.reader %s-%d is closed, error: %q", r.group, i, err.Error())
				return
			}
		})
	}
}

// fetchLoop 持续获取消息并处理
func (r *Reader) fetchLoop(handle func(msg kafka.Message)) error {
	for {
		msg, err := r.reader.FetchMessage(context.Background())
		// io.EOF means consumer closed
		// io.ErrClosedPipe means committing messages on the consumer,
		// kafka will refire the messages on uncommitted messages, ignore
		if err == io.EOF || errors.Is(err, io.ErrClosedPipe) {
			return err
		}
		if err != nil {
			r.errorLogger.Printf("kafka.reader error on reading message: %q", err.Error())
			continue
		}

		handle(msg)
	}
}

// Name 返回 Reader 读取的 topic 名称
func (r *Reader) Name() string {
	return r.topic
}

// Group 返回消费者组 ID
func (r *Reader) Group() string {
	return r.group
}
