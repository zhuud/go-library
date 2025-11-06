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
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zhuud/go-library/utils"
)

const (
	// 操作key要在同一节点上 {}, key要保存到那个节点上时,是根据'{}它里面的值来计算
	queueKey          = "{kafka:delay:queue}:%s:%d:%s"
	delayQueueName    = "delayed"
	reservedQueueName = "reserved"
	queueKeyBucketNum = 10
)

var (
	client  *redis.Redis
	metrics *stat.Metrics
	// 队列redis名称前缀
	queuePrefix = ""
	// 从队列中获取数据的批量大小
	queueGetBatchSize = 1000
	// 最大重试次数，默认 3 次
	maxRetryAttempts = 3
	// 重试延迟时间，默认 1 分钟
	retryDelayDuration = time.Minute

	//go:embed pop.lua
	popLuaScript string
	popScript    = redis.NewScript(popLuaScript)

	//go:embed release.lua
	releaseLuaScript string
	releaseScript    = redis.NewScript(releaseLuaScript)
)

type (
	DelayData struct {
		Topic     string `json:"topic"`
		Data      any    `json:"data"`
		Timestamp int64  `json:"timestamp"`
		Attempts  int    `json:"attempts"` // 重试次数 重试1次就+1
	}
)

// SetUp 设置延迟队列（使用默认重试配置）
func SetUp(redis *redis.Redis, batchSize int, prefix string) {
	SetUpWithRetry(redis, batchSize, prefix, 0, 0)
}

// SetUpWithRetry 设置延迟队列，支持配置重试参数
func SetUpWithRetry(redis *redis.Redis, batchSize int, prefix string, maxAttempts int, retryDelay time.Duration) {
	client = redis
	metrics = stat.NewMetrics("kafka.delay")
	queuePrefix = prefix
	queueGetBatchSize = batchSize

	if maxAttempts > 0 {
		maxRetryAttempts = maxAttempts
	}
	if retryDelay > 0 {
		retryDelayDuration = retryDelay
	}
}

func Push(ctx context.Context, topic string, data any, delayDuration time.Duration) error {
	if client == nil {
		return fmt.Errorf("kafka.delay.Push delay queue must setup before")
	}
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

	_, err = client.ZaddCtx(ctx, fmtQueueKey(ts, delayQueueName), ts, string(dj))
	if err != nil {
		return fmt.Errorf("kafka.delay.Push ZaddCtx error: %w", err)
	}

	return nil
}

func Pop() []string {
	ts := time.Now().Unix()

	list := make([]string, 0)
	if client == nil {
		logx.Error("kafka.delay.Pop client nil")
		return list
	}

	var mu sync.Mutex
	group := threading.NewRoutineGroup()

	for i := 0; i < queueKeyBucketNum; i++ {
		bucket := i
		group.Run(func() {
			data, err := client.ScriptRun(popScript,
				[]string{
					fmtQueueKey(int64(bucket), delayQueueName),
					fmtQueueKey(int64(bucket), reservedQueueName),
				}, []string{
					cast.ToString(ts),
					cast.ToString(queueGetBatchSize),
				})
			if err != nil {
				logx.Errorf("kafka.delay.Pop bucket: %d, ScriptRun error %v", bucket, err)
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

func SuccessAck(timestamp int64, taskJson string, startTime time.Duration) error {
	// 逻辑上不会有这种情况
	if len(taskJson) == 0 {
		return fmt.Errorf("kafka.delay.SuccessAck taskJson is empty")
	}

	err := removeFromReserved(timestamp, taskJson)
	recordMetrics(startTime, err != nil)

	return err
}

func FailAck(timestamp int64, taskJson string, startTime time.Duration) error {
	// 逻辑上不会有这种情况
	if len(taskJson) == 0 {
		return fmt.Errorf("kafka.delay.FailAck taskJson is empty")
	}

	// 解析当前任务数据
	var delayData DelayData
	err := json.Unmarshal([]byte(taskJson), &delayData)
	// 逻辑上不会有这种情况
	if err != nil {
		logx.Errorf("kafka.delay.FailAck Unmarshal taskJson: %s, error: %v", taskJson, err)
		_ = removeFromReserved(timestamp, taskJson)
		recordMetrics(startTime, true)
		return fmt.Errorf("kafka.delay.FailAck Unmarshal error: %w", err)
	}

	// 检查是否超过最大重试次数
	if delayData.Attempts >= maxRetryAttempts {
		logx.Errorf("kafka.delay.FailAck max retry attempts exceeded, topic: %s, attempts: %d, max: %d",
			delayData.Topic, delayData.Attempts, maxRetryAttempts)
		// 超过最大重试次数，直接删除原值，不再重试
		err = removeFromReserved(timestamp, taskJson)
		recordMetrics(startTime, true)
		return err
	}

	// 增加重试次数并计算新的延迟时间
	delayData.Attempts++
	newTimestamp := time.Now().Add(retryDelayDuration).Unix()
	delayData.Timestamp = newTimestamp

	// 生成新的 taskJson（注意：内容已更新，与原 taskJson 不同）
	newTaskJson, err := json.Marshal(delayData)
	// 逻辑上不会有这种情况
	if err != nil {
		logx.Errorf("kafka.delay.FailAck Marshal delayData: %+v, error: %v", delayData, err)
		recordMetrics(startTime, true)
		return fmt.Errorf("kafka.delay.FailAck Marshal error: %w", err)
	}

	// 原子操作：删除旧值，添加新值
	err = retry.Do(func() error {
		_, err := client.ScriptRun(releaseScript,
			[]string{
				fmtQueueKey(newTimestamp, delayQueueName),
				fmtQueueKey(timestamp, reservedQueueName),
			}, []string{
				taskJson,                    // ARGV[1]: 旧的 taskJson（用于删除）
				cast.ToString(newTimestamp), // ARGV[2]: 新的 timestamp
				string(newTaskJson),         // ARGV[3]: 新的 taskJson（用于添加）
			})

		return err
	}, retry.Attempts(2), retry.Delay(100*time.Millisecond))

	recordMetrics(startTime, true)

	return err
}

// removeFromReserved 从 reserved 队列中删除任务
func removeFromReserved(timestamp int64, taskJson string) error {
	return retry.Do(func() error {
		_, err := client.Zrem(fmtQueueKey(timestamp, reservedQueueName), taskJson)
		return err
	}, retry.Attempts(2), retry.Delay(100*time.Millisecond))
}

// recordMetrics 记录监控指标
func recordMetrics(startTime time.Duration, drop bool) {
	st := stat.Task{
		Duration: utils.Since(startTime),
		Drop:     drop,
	}
	metrics.Add(st)
}

func fmtQueueKey(timestamp int64, queueType string) string {
	d := int(timestamp % int64(queueKeyBucketNum))
	return fmt.Sprintf(queueKey, queuePrefix, d, queueType)
}
