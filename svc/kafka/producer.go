package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
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
)

var (
	producers = map[string]*internal.Writer{}
	mu        sync.Mutex
)

func Push(ctx context.Context, topic string, data any, sync ...bool) error {
	// 根据参数决定异步或同步模式（第一次初始化时确定）
	producer, err := NewProducer(topic, sync...)
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

/*
Kafka Writer 配置参数说明：
1. 基础配置
  - Addr (net.Addr, 必需): Kafka 集群地址，使用 kafka.TCP("host1:9092", "host2:9092")
  - Topic (string): 目标主题名称（与 Message.Topic 二选一）
  - AllowAutoTopicCreation (bool, 默认 false): 是否允许自动创建主题（生产环境建议关闭）

2. 分区负载均衡
  - Balancer (Balancer, 默认 RoundRobin): 分区负载均衡策略
    常用 Balancer 类型：
  - &kafka.LeastBytes{} - 最少字节数（推荐，当前默认）
  - &kafka.Hash{} - 哈希分区（与 Sarama 兼容）
  - kafka.CRC32Balancer{} - CRC32 哈希（与 librdkafka 兼容）
  - kafka.Murmur2Balancer{} - Murmur2 哈希（与 Java 客户端兼容）
  - &kafka.RoundRobin{} - 轮询（默认）

3. 批次配置（性能关键）
  - BatchSize (int, 默认 100): 每个批次的消息数量
  - BatchBytes (int64, 默认 1048576 即 1MB): 每个批次的最大字节数
  - BatchTimeout (time.Duration, 默认 1s): 批次刷新超时时间
    性能调优建议：
  - 高吞吐场景：增大 BatchSize 和 BatchBytes，适当增加 BatchTimeout
  - 低延迟场景：减小 BatchSize 和 BatchTimeout，优先保证实时性
  - 平衡场景：使用推荐配置

4. 超时配置
  - ReadTimeout (time.Duration, 默认 10s): 读取操作的超时时间
  - WriteTimeout (time.Duration, 默认 10s): 写入操作的超时时间

5. 重试与容错
  - MaxAttempts (int, 默认 10): 消息发送失败的最大重试次数
  - WriteBackoffMin (time.Duration, 默认 100ms): 重试前的最小退避时间
  - WriteBackoffMax (time.Duration, 默认 1s): 重试前的最大退避时间

6. 确认机制（可靠性关键）
  - RequiredAcks (RequiredAcks, 默认 RequireNone 即 0): 消息确认要求
    RequiredAcks 选项：
  - kafka.RequireNone (0) - 不等待确认（最快，但可能丢消息）
  - kafka.RequireOne (1) - 等待 Leader 确认（平衡，当前默认）
  - kafka.RequireAll (-1) - 等待所有 ISR 副本确认（最可靠，但最慢）

7. 异步模式
  - Async (bool, 默认 false): 是否启用异步模式
  - Completion (func([]Message, error)): 异步完成回调函数
    异步模式说明：
  - Async = false：同步模式，WriteMessages 会阻塞直到消息写入完成
  - Async = true：异步模式，WriteMessages 立即返回，通过 Completion 回调处理结果

8. 压缩配置
  - Compression (Compression, 默认 None): 消息压缩算法
    压缩算法选项：
  - kafka.CompressionNone - 不压缩
  - kafka.CompressionGzip - Gzip 压缩（压缩率高，CPU 消耗大）
  - kafka.CompressionSnappy - Snappy 压缩（推荐，平衡压缩率和性能，当前默认）
  - kafka.CompressionLz4 - LZ4 压缩（速度快，压缩率中等）
  - kafka.CompressionZstd - Zstd 压缩（压缩率高，性能好）
*/
func NewProducer(topic string, sync ...bool) (*internal.Writer, error) {
	// TODO 使用资源管理器
	producer, ok := producers[topic]
	if ok {
		return producer, nil
	}

	mu.Lock()
	defer func() {
		proc.AddShutdownListener(func() {
			_ = producers[topic].Close()
		})
		mu.Unlock()
	}()

	servers, err := getServers()
	if err != nil {
		return nil, err
	}

	opts := []internal.PushOptionFunc{
		internal.WithBatchSize(100),
		internal.WithBatchBytes(1024 * 1024),
		internal.WithBatchTimeout(50 * time.Millisecond),
		internal.WithAsync(true),
	}
	// 如果需要同步推送模式，则设置为同步模式
	if len(sync) > 0 && sync[0] {
		opts = append(opts, internal.WithAsync(false))
	}
	if conf.IsLocal() {
		opts = append(opts, internal.WithAllowAutoTopicCreation())
	}

	producer = internal.NewWriter(servers, topic, opts...)

	producers[topic] = producer

	return producers[topic], nil
}
