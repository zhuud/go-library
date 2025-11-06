package kafka

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stat"
)

// Consume
// Brokers: kafka 的多个 Broker 节点
// Group：消费者组
// Topic：订阅的 Topic 主题
// Offset：如果新的 topic kafka 没有对应的 offset 信息,或者当前的 offset 无效了(历史数据被删除),那么需要指定从头(first)消费还是从尾(last)部消费
// Conns: 一个 kafka queue 对应可对应多个 consumer，Conns 对应 kafka queue 数量，可以同时初始化多个 kafka queue，默认只启动一个
// Consumers : go-queue 内部是起多个 goroutine 从 kafka 中获取信息写入进程内的 channel，这个参数是控制此处的 goroutine 数量（⚠️ 并不是真正消费时的并发 goroutine 数量）
// Processors: 当 Consumers 中的多个 goroutine 将 kafka 消息拉取到进程内部的 channel 后，我们要真正消费消息写入我们自己逻辑，go-queue 内部通过此参数控制当前消费的并发 goroutine 数量
// MinBytes: fetch 一次返回的最小字节数,如果不够这个字节数就等待（最多等待 MaxWait 时间）一次返回的最大字节数,如果第一条消息的大小超过了这个限制仍然会继续拉取保证 consumer 的正常运行.因此并不是一个绝对的配置,消息的大小还需要受到 broker 的message.max.bytes限制,以及 topic 的max.message.bytes的限制
//   - kafka-go 库默认值：1字节
//   - go-queue/kq 默认值：10240字节 10K（通过 KqConf.MinBytes 配置）
// MaxBytes: fetch
// CommitInOrder: 顺序处理
//
// 可选参数说明（通过 kq.WithXxx 配置）:
// - WithMaxWait: 从 kafka 批量获取数据时，等待新数据到来的最大时间
//   - kafka-go 库默认值：10 秒（如果未配置）
//   - go-queue/kq 默认值：1 秒（通过 ensureQueueOptions 设置）
//   - 与 MinBytes 配合：数据量达到 MinBytes 或等待时间达到 MaxWait 时返回
//   - 当前配置：50ms（实时性优先）
//
// - WithCommitInterval: 提交 offset 到 kafka broker 的间隔时间
//   - 默认值：1 秒（go-queue/kq）
//   - 建议设置为 MaxWait 的 4-10 倍，减少重复消费风险
//   - 当前配置：200ms（MaxWait=50ms 的 4 倍）
func Consume(c kq.KqConf, consumerName string, handler kq.ConsumeHandler) {

	servers, err := getServers()
	if err != nil {
		log.Fatalf("kafka.Consume servers empty error: %v", err)
	}
	c.Brokers = servers

	serviceGroup := service.NewServiceGroup()
	defer serviceGroup.Stop()

	topics := strings.Split(c.Topic, ",")
	for _, topic := range topics {
		c.Topic = topic
		// 同一group 消费一份数据
		c.Group = c.Name + ":" + consumerName

		mq := kq.MustNewQueue(c, handler,
			kq.WithMaxWait(50*time.Millisecond),
			kq.WithCommitInterval(200*time.Millisecond),
			kq.WithMetrics(stat.NewMetrics(c.Group)),
			kq.WithErrorHandler(func(ctx context.Context, msg kafka.Message, err error) {
				logx.WithContext(ctx).WithCallerSkip(1).Errorf("kafka.Consume %s ,\n message: %+v ,\n error: %v", c.Group, msg, err)
			}),
		)
		serviceGroup.Add(mq)
	}

	log.Printf("Starting Mq Server At %s, Topics: %v ...", c.Brokers, topics)
	serviceGroup.Start()
}
