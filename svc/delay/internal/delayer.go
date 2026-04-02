package internal

// Delayer 是 delay queue 的核心实现。
// 使用 Redis Sorted Set（`delayed`/`reserved`）+ Lua 脚本保证原子出队/重投递：
// - `Push`：把消息写入 `delayed`，score=目标触发时间（unix seconds）
// - `pop`：从 `delayed` 中取出到期元素，并原子迁移到 `reserved`
// - `successAck`：成功消费后从 `reserved` 删除
// - `failAck`：失败时根据重试策略把消息重投递到 `delayed`（通过 Lua 原子替换 reserved）
// - `removeStaleReserved`：当消费者异常导致 reserved 未 ack 时，定期清除

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

const (
	// 生命周期状态：New → Running → Stopped（单向不可逆）
	stateNew     int32 = iota // 已创建，未启动
	stateRunning              // 消费循环运行中
	stateStopped              // 已停止，终态

	// 队列 key 格式：{delay:queue:prefix}:queueType
	queueKey          = "{delay:queue:%s}:%s"
	delayQueueName    = "delayed"
	reservedQueueName = "reserved"

	// 默认配置
	defaultMaxPushDelayDuration = time.Hour * 24 * 7 // 默认最大推送延迟时长
	defaultConsumeInterval      = time.Millisecond * 100 // 默认消费轮询间隔
	defaultBatchSize            = 100 // 默认单次消费数量
	defaultConcurrency          = 100 // 默认并发处理协程数
	defaultHandlerTimeout       = time.Second * 5 // 默认单条消息处理超时时间
	defaultMaxRetryAttempts     = 0 // 默认最大重试次数
	defaultRetryDelay           = time.Second * 30 // 默认重试延迟时间
	defaultReservedTimeout      = time.Minute * 10 // 默认 reserved 队列中消息的超时时间
)

var (
	//go:embed delayer_pop.lua
	popLuaScript string
	popScript    = redis.NewScript(popLuaScript)

	//go:embed delayer_release.lua
	releaseLuaScript string
	releaseScript    = redis.NewScript(releaseLuaScript)
)

type (
	// Message 延迟消息。
	Message struct {
		ID        string `json:"id"`
		Key       string `json:"key"`
		Data      any    `json:"data"`
		Timestamp int64  `json:"timestamp"`
		Attempts  int    `json:"attempts"`
	}

	// MessageHandler 消息到期处理回调。
	MessageHandler func(ctx context.Context, msg *Message) error

	// OptionFunc 配置选项函数。
	OptionFunc func(config *Config)

	// Config 延迟队列配置。
	Config struct {
		// Prefix 用于拼接 Redis Key 的业务前缀。
		Prefix string

		// MaxPushDelayDuration 限制单次 Push 允许的最大延迟时长。
		MaxPushDelayDuration time.Duration

		// ConsumeInterval 消费轮询间隔。
		ConsumeInterval time.Duration

		// BatchSize 单次从 delayed 弹出的最大条数。
		BatchSize int

		// Concurrency 处理到期消息的并发度。
		Concurrency int

		// HandlerTimeout 单条消息处理超时时间。
		HandlerTimeout time.Duration

		// MaxRetryAttempts 失败重试次数。
		// 为 0 表示默认不重试（第一次失败直接从 reserved 删除）。
		MaxRetryAttempts int

		// RetryDelayDuration 重试延迟时长。
		RetryDelayDuration time.Duration

		// ReservedTimeout reserved 队列中消息的超时时间，超时未 ack 的消息将被直接删除并记录 error。
		ReservedTimeout time.Duration
	}

	// Delayer 延迟队列实例。
	Delayer struct {
		mu     sync.Mutex
		state  int32
		cancel context.CancelFunc
		wg     sync.WaitGroup

		client        *redis.Redis
		metrics       *stat.Metrics
		popScript     *redis.Script
		releaseScript *redis.Script
		logger        *delayLogger

		prefix               string
		maxPushDelayDuration time.Duration
		consumeInterval      time.Duration
		batchSize            int
		concurrency          int
		handlerTimeout       time.Duration
		maxRetryAttempts     int
		retryDelayDuration   time.Duration
		reservedTimeout      time.Duration
	}
)

// NewDelayer 创建新的延迟队列实例
func NewDelayer(redisClient *redis.Redis, opts ...OptionFunc) *Delayer {
	if redisClient == nil {
		panic("delay: redis client cannot be nil")
	}

	config := Config{
		MaxPushDelayDuration: defaultMaxPushDelayDuration,
		ConsumeInterval:      defaultConsumeInterval,
		BatchSize:            defaultBatchSize,
		Concurrency:          defaultConcurrency,
		HandlerTimeout:       defaultHandlerTimeout,
		MaxRetryAttempts:     defaultMaxRetryAttempts,
		RetryDelayDuration:   defaultRetryDelay,
		ReservedTimeout:      defaultReservedTimeout,
	}

	for _, opt := range opts {
		opt(&config)
	}

	if len(config.Prefix) == 0 {
		panic("delay prefix cannot be empty")
	}

	return &Delayer{
		client:        redisClient,
		metrics:       stat.NewMetrics(fmt.Sprintf("delay.%s", config.Prefix)),
		popScript:     popScript,
		releaseScript: releaseScript,
		logger:        newDelayLogger(fmt.Sprintf("delay.%s", config.Prefix)),

		prefix:               config.Prefix,
		maxPushDelayDuration: config.MaxPushDelayDuration,
		consumeInterval:      config.ConsumeInterval,
		batchSize:            config.BatchSize,
		concurrency:          config.Concurrency,
		handlerTimeout:       config.HandlerTimeout,
		maxRetryAttempts:     config.MaxRetryAttempts,
		retryDelayDuration:   config.RetryDelayDuration,
		reservedTimeout:      config.ReservedTimeout,
	}
}

// Push 推送延迟消息
func (dl *Delayer) Push(ctx context.Context, key string, data any, delayDuration time.Duration) error {
	if len(key) == 0 {
		return fmt.Errorf("delay.Push key must not be empty")
	}
	if delayDuration < time.Second || delayDuration > dl.maxPushDelayDuration {
		return fmt.Errorf("delay.Push delayDuration must be at least 1 second and at most %v", dl.maxPushDelayDuration)
	}

	ts := time.Now().Add(delayDuration).Unix()
	msg := Message{
		ID:        utils.GenUniqId(),
		Key:       key,
		Data:      data,
		Timestamp: ts,
		Attempts:  0,
	}
	mj, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("delay.Push Marshal error: %w", err)
	}

	_, err = dl.client.ZaddCtx(ctx, dl.fmtQueueKey(delayQueueName), ts, string(mj))
	if err != nil {
		return fmt.Errorf("delay.Push ZaddCtx error: %w", err)
	}

	return nil
}

// Start 启动后台消费。只能调用一次，重复调用或已 Stop 后调用会被忽略。
func (dl *Delayer) Start(handler MessageHandler) {
	if handler == nil {
		panic("delay.Start handler cannot be nil")
	}

	dl.mu.Lock()
	if dl.state != stateNew {
		dl.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	dl.cancel = cancel
	dl.state = stateRunning
	dl.wg.Add(1)
	dl.mu.Unlock()

	dl.logger.Infof("delay consumer start in the background")
	go func() {
		defer dl.wg.Done()
		dl.loopFetch(ctx, handler)
	}()
}

// Stop 停止后台消费并等待退出。幂等安全，多次调用不会 panic。
func (dl *Delayer) Stop() {
	dl.mu.Lock()
	if dl.state != stateRunning {
		dl.mu.Unlock()
		return
	}
	dl.state = stateStopped
	dl.mu.Unlock()

	dl.cancel()
	dl.wg.Wait()
}

func (dl *Delayer) loopFetch(ctx context.Context, handler MessageHandler) {
	defer dl.logger.Infof("delay consumer stop")

	ticker := time.NewTicker(dl.consumeInterval)
	defer ticker.Stop()

	var lastClean time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dl.consumeOnce(ctx, handler, &lastClean)
		}
	}
}

// consumeOnce 单次消费（含 reserved 超时清理），自带 panic 恢复，确保 loopFetch 循环不因 panic 中断
func (dl *Delayer) consumeOnce(ctx context.Context, handler MessageHandler, lastClean *time.Time) {
	defer func() {
		if err := recover(); err != nil {
			dl.logger.Errorf("delay.consumeOnce panic: %v", err)
		}
	}()

	// 清理超时未 ack 的消息
	if time.Since(*lastClean) >= dl.reservedTimeout/2 {
		dl.removeStaleReserved()
		*lastClean = time.Now()
	}

	// 消费消息
	taskJsonList := dl.pop()
	if len(taskJsonList) == 0 {
		return
	}

	runner := threading.NewTaskRunner(dl.concurrency)
	for _, taskJson := range taskJsonList {
		task := taskJson
		runner.Schedule(func() {
			dl.process(ctx, handler, task)
		})
	}
	runner.Wait()
}

// pop 从 delayed 中取出到期消息并原子迁移到 reserved。
// Lua 脚本返回的是 reserved 中的 taskJson 列表，供后续 process 处理。
func (dl *Delayer) pop() []string {
	ts := time.Now().Unix()

	data, err := dl.client.ScriptRun(dl.popScript,
		[]string{
			dl.fmtQueueKey(delayQueueName),
			dl.fmtQueueKey(reservedQueueName),
		}, []string{
			cast.ToString(ts),
			cast.ToString(dl.batchSize),
		})
	if err != nil {
		dl.logger.Errorf("delay.pop ScriptRun error: %v", err)
		return nil
	}

	return cast.ToStringSlice(data)
}

func (dl *Delayer) process(ctx context.Context, handler MessageHandler, taskJson string) {
	defer func() {
		if err := recover(); err != nil {
			dl.logger.Errorf("delay.process data: %s, panic: %v", taskJson, err)
		}
	}()

	now := utils.Now()
	var msg Message
	err := json.Unmarshal([]byte(taskJson), &msg)
	if err != nil {
		dl.logger.Errorf("delay.process Unmarshal data: %s, error: %v", taskJson, err)
		if ackErr := dl.successAck(taskJson, now); ackErr != nil {
			dl.logger.Errorf("delay.process Unmarshal data: %s, successAck error: %v", taskJson, ackErr)
		}
		return
	}

	if len(msg.Key) == 0 {
		dl.logger.Errorf("delay.process invalid empty key, data: %s", taskJson)
		if ackErr := dl.successAck(taskJson, now); ackErr != nil {
			dl.logger.Errorf("delay.process invalid empty key, data: %s, successAck error: %v", taskJson, ackErr)
		}
		return
	}

	dl.logger.Infof("delay.process forward message: %+v", msg)

	handlerCtx, cancel := context.WithTimeout(ctx, dl.handlerTimeout)
	defer cancel()

	err = handler(handlerCtx, &msg)
	if err != nil {
		dl.logger.Errorf("delay.process handler message: %+v, error: %v", msg, err)
		if ackErr := dl.failAck(taskJson, now); ackErr != nil {
			dl.logger.Errorf("delay.process handler message: %+v, failAck error: %v", msg, ackErr)
		}
	} else {
		if ackErr := dl.successAck(taskJson, now); ackErr != nil {
			dl.logger.Errorf("delay.process handler message: %+v, successAck error: %v", msg, ackErr)
		}
	}
}

func (dl *Delayer) successAck(taskJson string, startTime time.Duration) error {
	if len(taskJson) == 0 {
		return fmt.Errorf("delay.successAck taskJson is empty")
	}

	err := dl.removeFromReserved(taskJson)
	dl.recordMetrics(startTime, err != nil)

	return err
}

func (dl *Delayer) failAck(taskJson string, startTime time.Duration) error {
	if len(taskJson) == 0 {
		return fmt.Errorf("delay.failAck taskJson is empty")
	}

	var msg Message
	err := json.Unmarshal([]byte(taskJson), &msg)
	if err != nil {
		removeErr := dl.removeFromReserved(taskJson)
		dl.recordMetrics(startTime, true)
		return fmt.Errorf("delay.failAck Unmarshal error: %w, removeFromReserved error: %w", err, removeErr)
	}

	if msg.Attempts >= dl.maxRetryAttempts {
		removeErr := dl.removeFromReserved(taskJson)
		dl.recordMetrics(startTime, true)
		return fmt.Errorf("delay.failAck max delivery attempts exceeded, max: %d, removeFromReserved error: %w", dl.maxRetryAttempts, removeErr)
	}

	msg.Attempts++
	newTimestamp := time.Now().Add(dl.retryDelayDuration).Unix()
	msg.Timestamp = newTimestamp

	newTaskJson, err := json.Marshal(msg)
	if err != nil {
		removeErr := dl.removeFromReserved(taskJson)
		dl.recordMetrics(startTime, true)
		return fmt.Errorf("delay.failAck Marshal error: %w, removeFromReserved error: %w", err, removeErr)
	}

	err = retry.Do(func() error {
		_, err := dl.client.ScriptRun(dl.releaseScript,
			[]string{
				dl.fmtQueueKey(reservedQueueName),
				dl.fmtQueueKey(delayQueueName),
			}, []string{
				taskJson,
				cast.ToString(newTimestamp),
				string(newTaskJson),
			})
		return err
	}, retry.Attempts(2), retry.Delay(10*time.Millisecond))

	dl.recordMetrics(startTime, true)

	return err
}

func (dl *Delayer) removeFromReserved(taskJson string) error {
	return retry.Do(func() error {
		_, err := dl.client.Zrem(dl.fmtQueueKey(reservedQueueName), taskJson)
		return err
	}, retry.Attempts(2), retry.Delay(10*time.Millisecond))
}

// removeStaleReserved 清理 reserved 队列中超时未 ack 的消息（直接删除 + 记录 error）
func (dl *Delayer) removeStaleReserved() {
	threshold := time.Now().Add(-dl.reservedTimeout).Unix()

	count, err := dl.client.Zremrangebyscore(dl.fmtQueueKey(reservedQueueName), 0, threshold)
	if err != nil {
		dl.logger.Errorf("delay.removeStaleReserved Zremrangebyscore error: %v", err)
		return
	}

	if count > 0 {
		dl.logger.Errorf("delay.removeStaleReserved removed %d stale messages from reserved", count)
	}
}

func (dl *Delayer) recordMetrics(startTime time.Duration, drop bool) {
	dl.metrics.Add(stat.Task{
		Duration: utils.Since(startTime),
		Drop:     drop,
	})
}

func (dl *Delayer) fmtQueueKey(queueType string) string {
	return fmt.Sprintf(queueKey, dl.prefix, queueType)
}

// WithPrefix 配置队列 redis 名称前缀
func WithPrefix(prefix string) OptionFunc {
	return func(config *Config) {
		config.Prefix = prefix
	}
}

// WithMaxPushDelay 配置 Push 允许的最大延迟时长，默认 7 天
func WithMaxPushDelay(d time.Duration) OptionFunc {
	return func(config *Config) {
		if d >= time.Second {
			config.MaxPushDelayDuration = d
		}
	}
}

// WithConsumeInterval 配置消费轮询间隔
func WithConsumeInterval(interval time.Duration) OptionFunc {
	return func(config *Config) {
		if interval > 0 {
			config.ConsumeInterval = interval
		}
	}
}

// WithBatchSize 配置单次消费数量
func WithBatchSize(size int) OptionFunc {
	return func(config *Config) {
		if size > 0 {
			config.BatchSize = size
		}
	}
}

// WithConcurrency 配置并发处理协程数
func WithConcurrency(concurrency int) OptionFunc {
	return func(config *Config) {
		if concurrency > 0 {
			config.Concurrency = concurrency
		}
	}
}

// WithHandlerTimeout 配置单条消息处理超时时间
func WithHandlerTimeout(d time.Duration) OptionFunc {
	return func(config *Config) {
		if d > 0 {
			config.HandlerTimeout = d
		}
	}
}

// WithMaxRetryAttempts 配置最大重试次数
func WithMaxRetryAttempts(attempts int) OptionFunc {
	return func(config *Config) {
		if attempts > 0 {
			config.MaxRetryAttempts = attempts
		}
	}
}

// WithRetryDelay 配置重试延迟时间
func WithRetryDelay(d time.Duration) OptionFunc {
	return func(config *Config) {
		if d > 0 {
			config.RetryDelayDuration = d
		}
	}
}

// WithReservedTimeout 配置 reserved 队列中消息的超时时间，超时未 ack 的消息将被直接删除并记录 error
func WithReservedTimeout(d time.Duration) OptionFunc {
	return func(config *Config) {
		if d >= time.Minute {
			config.ReservedTimeout = d
		}
	}
}
