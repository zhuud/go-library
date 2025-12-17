package internal

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zhuud/go-library/utils"
)

type (
	// DelayData 延迟队列数据
	DelayData struct {
		Topic     string `json:"topic"`
		Data      any    `json:"data"`
		Timestamp int64  `json:"timestamp"`
		Attempts  int    `json:"attempts"` // 重试次数 重试1次就+1
	}

	// DelayOptionFunc 延迟队列配置选项函数
	DelayOptionFunc func(config *DelayConf)

	// DelayConf 延迟队列配置结构体
	DelayConf struct {
		// 队列redis名称前缀
		Prefix string
		// 从队列中获取数据的批量大小
		BatchSize int
		// 最大重试次数
		MaxRetryAttempts int
		// 重试延迟时间
		RetryDelayDuration time.Duration
		// 并发处理协程数，默认 10
		Concurrency int
	}

	// Delayer 延迟队列
	// 支持延迟消息推送
	// 支持消息重试机制
	// 支持 metrics 统计
	Delayer struct {
		client             *redis.Redis
		metrics            *stat.Metrics
		prefix             string
		batchSize          int
		maxRetryAttempts   int
		retryDelayDuration time.Duration
		concurrency        int // 并发处理协程数
		popScript          *redis.Script
		releaseScript      *redis.Script
		logger             *delayLogger
	}

	// MessageHandler 消息处理函数
	// 返回 error 表示处理失败，将触发重试机制
	MessageHandler func(ctx context.Context, delayData *DelayData) error
)

const (
	// 原子操作key要在同一节点上 {}, key要保存到那个节点上时,是根据'{}它里面的值来计算
	queueKey          = "{kafka:delay:queue:%s}:%d:%s"
	delayQueueName    = "delayed"
	reservedQueueName = "reserved"
	queueKeyBucketNum = 10

	// 默认配置值
	// 从延迟队列中取出多少处理
	defaultBatchSize = 1000
	// 延迟任务失败重试次数
	defaultMaxRetryAttempts = 2
	// 延迟任务失败重试时间
	defaultRetryDelay = time.Minute
	// 默认并发处理协程数
	defaultConcurrency = 100
)

var (
	//go:embed delayer_pop.lua
	popLuaScript string
	popScript    = redis.NewScript(popLuaScript)

	//go:embed delayer_release.lua
	releaseLuaScript string
	releaseScript    = redis.NewScript(releaseLuaScript)
)

// NewDelayer 创建新的延迟队列实例
func NewDelayer(redisClient *redis.Redis, opts ...DelayOptionFunc) *Delayer {
	if redisClient == nil {
		panic("kafka.delay redis client cannot be nil")
	}

	config := DelayConf{
		BatchSize:          defaultBatchSize,
		MaxRetryAttempts:   defaultMaxRetryAttempts,
		RetryDelayDuration: defaultRetryDelay,
		Concurrency:        defaultConcurrency,
	}

	for _, opt := range opts {
		opt(&config)
	}

	return &Delayer{
		client:             redisClient,
		metrics:            stat.NewMetrics("kafka.delay"),
		prefix:             config.Prefix,
		batchSize:          config.BatchSize,
		maxRetryAttempts:   config.MaxRetryAttempts,
		retryDelayDuration: config.RetryDelayDuration,
		concurrency:        config.Concurrency,
		popScript:          popScript,
		releaseScript:      releaseScript,
		logger:             newDelayLogger(),
	}
}

// Push 推送延迟消息到队列
func (dl *Delayer) Push(ctx context.Context, topic string, data any, delayDuration time.Duration) error {
	if len(topic) == 0 {
		return fmt.Errorf("kafka.delay.Push topic must not be empty")
	}
	if delayDuration < time.Second || delayDuration > time.Hour*24*7 {
		return fmt.Errorf("kafka.delay.Push delayDuration must be at least 1 second and at most 7 days")
	}

	ts := time.Now().Add(delayDuration).Unix()
	dd := DelayData{
		Topic:     topic,
		Data:      data,
		Timestamp: ts,
		Attempts:  0,
	}
	dj, err := json.Marshal(dd)
	if err != nil {
		return fmt.Errorf("kafka.delay.Push Marshal error: %w", err)
	}

	_, err = dl.client.ZaddCtx(ctx, dl.fmtQueueKey(ts, delayQueueName), ts, string(dj))
	if err != nil {
		return fmt.Errorf("kafka.delay.Push ZaddCtx error: %w", err)
	}

	return nil
}

// Pop 从延迟队列中弹出到期的消息
func (dl *Delayer) Pop() []string {
	ts := time.Now().Unix()
	list := make([]string, 0)

	var mu sync.Mutex
	group := threading.NewRoutineGroup()

	for i := 0; i < queueKeyBucketNum; i++ {
		bucket := i
		group.Run(func() {
			data, err := dl.client.ScriptRun(dl.popScript,
				[]string{
					dl.fmtQueueKey(int64(bucket), delayQueueName),
					dl.fmtQueueKey(int64(bucket), reservedQueueName),
				}, []string{
					cast.ToString(ts),
					cast.ToString(dl.batchSize),
				})
			if err != nil {
				dl.logger.Errorf("kafka.delay.Pop bucket: %d, ScriptRun error %v", bucket, err)
				return
			}
			item := cast.ToStringSlice(data)
			if len(item) > 0 {
				mu.Lock()
				list = append(list, item...)
				mu.Unlock()
			}
		})
	}

	group.Wait()
	return list
}

// Start 启动后台消费，处理到期的延迟消息
// handler: 消息处理函数，返回 error 表示处理失败，将触发重试机制
// interval: 消费间隔时间，默认 1 秒
func (dl *Delayer) Start(handler MessageHandler, interval time.Duration) {
	if handler == nil {
		panic("kafka.delay message handler cannot be nil")
	}
	if interval <= 0 {
		interval = time.Second
	}
	dl.logger.Infof("kafka.delay consumer start in the background")
	go dl.loopFetch(handler, interval)
}

// Stop 停止后台消费（当前实现为空，程序退出时 goroutine 会自动终止）
func (dl *Delayer) Stop() {
	// 程序退出时 goroutine 会自动终止，无需额外处理
}

// loop 后台消费循环
func (dl *Delayer) loopFetch(handler MessageHandler, interval time.Duration) {
	defer func() {
		if err := recover(); err != nil {
			dl.logger.Errorf("kafka.delay consumer panic: %v", err)
		}
		dl.logger.Infof("kafka.delay consumer stop")
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		taskJsonList := dl.Pop()
		if len(taskJsonList) == 0 {
			// TODO 后续去掉打印
			dl.logger.Infof("kafka.delay consumer no data to process")
			continue
		}

		// 使用 go-zero 的 TaskRunner 控制并发协程数，避免创建过多协程
		runner := threading.NewTaskRunner(dl.concurrency)
		for _, taskJson := range taskJsonList {
			task := taskJson // 避免闭包问题，使用局部变量捕获循环变量
			runner.Schedule(func() {
				// TODO 后续去掉打印
				dl.logger.Infof("kafka.delay consumer process data: %s", task)
				dl.process(handler, task)
			})
		}
		runner.Wait()
	}
}

// process 处理单条消息，包含解析、校验、调用 handler、Ack 等完整流程
func (dl *Delayer) process(handler MessageHandler, taskJson string) {
	// 加个recover处理，避免单条消息处理失败影响整个循环
	defer func() {
		if err := recover(); err != nil {
			dl.logger.Errorf("kafka.delay.process data: %s, handler: %+v, panic: %v", taskJson, handler, err)
		}
	}()

	now := utils.Now()
	var delayData DelayData
	err := json.Unmarshal([]byte(taskJson), &delayData)
	if err != nil {
		dl.logger.Errorf("kafka.delay.process Unmarshal data: %s, error: %v", taskJson, err)
		// 无法解析的数据直接删除
		_ = dl.SuccessAck(0, taskJson, now)
		return
	}

	// 校验必填字段
	if len(delayData.Topic) == 0 {
		dl.logger.Errorf("kafka.delay.process invalid data: %s", taskJson)
		// 无效数据直接删除，避免堆积
		_ = dl.SuccessAck(delayData.Timestamp, taskJson, now)
		return
	}

	// 调用外部传入的消息处理函数
	dl.logger.Infof("kafka.delay.process forward delay message, topic: %s, timestamp: %d, data: %s", delayData.Topic, time.Now().Unix(), taskJson)
	err = handler(context.Background(), &delayData)
	if err != nil {
		dl.logger.Errorf("kafka.delay.process handler error, topic: %s, data: %s, error: %v", delayData.Topic, taskJson, err)
		_ = dl.FailAck(delayData.Timestamp, taskJson, now)
	} else {
		_ = dl.SuccessAck(delayData.Timestamp, taskJson, now)
	}
}

// SuccessAck 确认消息处理成功
func (dl *Delayer) SuccessAck(timestamp int64, taskJson string, startTime time.Duration) error {
	if len(taskJson) == 0 {
		return fmt.Errorf("kafka.delay.SuccessAck taskJson is empty")
	}

	err := dl.removeFromReserved(timestamp, taskJson)
	dl.recordMetrics(startTime, err != nil)

	return err
}

// FailAck 确认消息处理失败，进行重试或丢弃
func (dl *Delayer) FailAck(timestamp int64, taskJson string, startTime time.Duration) error {
	if len(taskJson) == 0 {
		return fmt.Errorf("kafka.delay.FailAck taskJson is empty")
	}

	// 解析当前任务数据
	var delayData DelayData
	err := json.Unmarshal([]byte(taskJson), &delayData)
	if err != nil {
		dl.logger.Errorf("kafka.delay.FailAck Unmarshal taskJson: %s, error: %v", taskJson, err)
		_ = dl.removeFromReserved(timestamp, taskJson)
		dl.recordMetrics(startTime, true)
		return fmt.Errorf("kafka.delay.FailAck Unmarshal error: %w", err)
	}

	// 检查是否超过最大重试次数
	if delayData.Attempts >= dl.maxRetryAttempts {
		dl.logger.Errorf("kafka.delay.FailAck max retry attempts exceeded, topic: %s, attempts: %d, max: %d",
			delayData.Topic, delayData.Attempts, dl.maxRetryAttempts)
		// 超过最大重试次数，直接删除原值，不再重试
		err = dl.removeFromReserved(timestamp, taskJson)
		dl.recordMetrics(startTime, true)
		return err
	}

	// 增加重试次数并计算新的延迟时间
	delayData.Attempts++
	newTimestamp := time.Now().Add(dl.retryDelayDuration).Unix()
	delayData.Timestamp = newTimestamp

	// 生成新的 taskJson（注意：内容已更新，与原 taskJson 不同）
	newTaskJson, err := json.Marshal(delayData)
	if err != nil {
		dl.logger.Errorf("kafka.delay.FailAck Marshal delayData: %+v, error: %v", delayData, err)
		dl.recordMetrics(startTime, true)
		return fmt.Errorf("kafka.delay.FailAck Marshal error: %w", err)
	}

	// 原子操作：删除旧值，添加新值
	err = retry.Do(func() error {
		_, err := dl.client.ScriptRun(dl.releaseScript,
			[]string{
				dl.fmtQueueKey(newTimestamp, delayQueueName),
				dl.fmtQueueKey(timestamp, reservedQueueName),
			}, []string{
				taskJson,                    // ARGV[1]: 旧的 taskJson（用于删除）
				cast.ToString(newTimestamp), // ARGV[2]: 新的 timestamp
				string(newTaskJson),         // ARGV[3]: 新的 taskJson（用于添加）
			})

		return err
	}, retry.Attempts(2), retry.Delay(10*time.Millisecond))

	dl.recordMetrics(startTime, true)

	return err
}

// removeFromReserved 从 reserved 队列中删除任务
func (dl *Delayer) removeFromReserved(timestamp int64, taskJson string) error {
	return retry.Do(func() error {
		_, err := dl.client.Zrem(dl.fmtQueueKey(timestamp, reservedQueueName), taskJson)
		return err
	}, retry.Attempts(2), retry.Delay(10*time.Millisecond))
}

// recordMetrics 记录监控指标
func (dl *Delayer) recordMetrics(startTime time.Duration, drop bool) {
	st := stat.Task{
		Duration: utils.Since(startTime),
		Drop:     drop,
	}
	dl.metrics.Add(st)
}

// fmtQueueKey 格式化队列key
func (dl *Delayer) fmtQueueKey(timestamp int64, queueType string) string {
	d := int(timestamp % int64(queueKeyBucketNum))
	return fmt.Sprintf(queueKey, dl.prefix, d, queueType)
}
