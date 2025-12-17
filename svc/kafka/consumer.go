package kafka

import (
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zhuud/go-library/svc/kafka/internal"
)

// ReaderOptionFunc 是 internal.ReaderOptionFunc 的类型别名，方便外部包使用
type (
	ReaderOptionFunc = internal.ReaderOptionFunc
	IConsumeHandler  = internal.ConsumeHandler

	// ConsumeHandler 消费消息的处理器接口
	ConsumeHandler interface {
		IConsumeHandler
		Name() string
		Topics() []string
	}
)

// Consume 启动 Kafka 消费者服务
func Consume(handler ConsumeHandler, opts ...ReaderOptionFunc) {
	brokers, err := internal.GetServers()
	if err != nil {
		log.Fatalf("kafka.Consume brokers empty error: %v", err)
	}
	if handler == nil {
		log.Fatalf("kafka.Consume handler not set")
	}
	if len(handler.Topics()) == 0 {
		log.Fatalf("kafka.Consume topics not set")
	}
	if len(handler.Name()) == 0 {
		log.Fatalf("kafka.Consume name not set")
	}

	serviceGroup := service.NewServiceGroup()
	defer serviceGroup.Stop()

	for _, topic := range handler.Topics() {
		// 每个 topic 使用独立的消费者组，便于独立管理和监控
		group := topic + ":" + handler.Name()

		// 创建 Reader，所有配置通过 opts 参数传入
		reader := newReader(brokers, topic, group, handler, opts...)
		serviceGroup.Add(reader)
	}

	log.Printf("Starting Mq Server At %v, Topics: %v ...", brokers, handler.Topics())
	serviceGroup.Start()
}

// newReader 创建 Reader 实例
func newReader(brokers []string, topic string, group string, handler ConsumeHandler, opts ...ReaderOptionFunc) *internal.Reader {
	// 从配置读取认证信息
	username, password := internal.GetSASL()
	if len(username) > 0 && len(password) > 0 {
		opts = append(opts, WithSASL(username, password))
	}
	caFile := internal.GetTLS()
	if len(caFile) > 0 {
		opts = append(opts, WithTLS(caFile))
	}
	return internal.NewReader(brokers, topic, group, handler, opts...)
}

// WithSASL 配置 SASL 认证
func WithSASL(username, password string) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.Username = username
		config.Password = password
	}
}

// WithTLS 配置 TLS 证书
func WithTLS(caFile string) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.CaFile = caFile
	}
}

// WithStartOffset 配置起始 offset
func WithStartOffset(offset int64) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.StartOffset = offset
	}
}

// WithMinBytes 配置最小字节数
func WithMinBytes(bytes int) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.MinBytes = bytes
	}
}

// WithMaxBytes 配置最大字节数
func WithMaxBytes(bytes int) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.MaxBytes = bytes
	}
}

// WithMaxWait 配置最大等待时间
func WithMaxWait(wait time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.MaxWait = wait
	}
}

// WithCommitInterval 配置提交间隔
func WithCommitInterval(interval time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.CommitInterval = interval
	}
}

// WithQueueCapacity 配置队列容量
func WithQueueCapacity(capacity int) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.QueueCapacity = capacity
	}
}

// WithReadBatchTimeout 配置读取批次超时
func WithReadBatchTimeout(timeout time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.ReadBatchTimeout = timeout
	}
}

// WithSessionTimeout 配置会话超时
func WithSessionTimeout(timeout time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.SessionTimeout = timeout
	}
}

// WithReaderMaxAttempts 配置最大尝试次数
func WithReaderMaxAttempts(attempts int) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.MaxAttempts = attempts
	}
}

// WithHeartbeatInterval 配置心跳间隔
func WithHeartbeatInterval(interval time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.HeartbeatInterval = interval
	}
}

// WithRebalanceTimeout 配置重平衡超时
func WithRebalanceTimeout(timeout time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.RebalanceTimeout = timeout
	}
}

// WithJoinGroupBackoff 配置加入组退避时间
func WithJoinGroupBackoff(backoff time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.JoinGroupBackoff = backoff
	}
}

// WithPartitionWatchInterval 配置分区监听间隔
func WithPartitionWatchInterval(interval time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.PartitionWatchInterval = interval
	}
}

// WithWatchPartitionChanges 配置是否监听分区变化
func WithWatchPartitionChanges(watch bool) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.WatchPartitionChanges = &watch
	}
}

// WithGroupBalancers 配置消费组平衡器
func WithGroupBalancers(balancers ...kafka.GroupBalancer) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.GroupBalancers = balancers
	}
}

// WithReadBackoffMin 配置读取退避最小时间
func WithReadBackoffMin(min time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.ReadBackoffMin = min
	}
}

// WithReadBackoffMax 配置读取退避最大时间
func WithReadBackoffMax(max time.Duration) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.ReadBackoffMax = max
	}
}

// WithProcessors 配置处理协程数量
func WithProcessors(processors int) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.Processors = processors
	}
}

// WithConsumers 配置消费协程数量
func WithConsumers(consumers int) ReaderOptionFunc {
	return func(config *internal.ReaderConf) {
		config.Consumers = consumers
	}
}
