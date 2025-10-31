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
)

var (
	client            *redis.Redis
	metrics           = stat.NewMetrics("kafka.delay")
	queuePrefix       = ""
	queueKeyBucketNum = 10
	// 改动须确保 原先key消费完成 或者实现key迁移
	queueGetBatchSize = -1

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
		Attempts  int    `json:"attempts"`
	}
)

func SetUp(redis *redis.Redis, batchSize int, prefix string) {
	client = redis
	queueGetBatchSize = batchSize
	queuePrefix = prefix
}

func Push(ctx context.Context, topic string, data any, delayDuration time.Duration) error {
	if client == nil {
		return fmt.Errorf("kafka.delay.Push delay queue must setup before")
	}

	ts := time.Now().Add(delayDuration).Unix()
	dd := DelayData{
		Topic:     topic,
		Data:      data,
		Timestamp: ts,
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
	if len(taskJson) == 0 {
		return fmt.Errorf("kafka.delay.SuccessAck taskJson is empty")
	}

	return retry.Do(func() error {
		_, err := client.Zrem(fmtQueueKey(timestamp, reservedQueueName), taskJson)
		// 监控qps 耗时
		metrics.Add(stat.Task{
			Duration: utils.Since(startTime),
		})
		return err
	}, retry.Attempts(2), retry.Delay(100*time.Millisecond))
}

func FailAck(timestamp int64, taskJson string, startTime time.Duration) error {
	if len(taskJson) == 0 {
		return fmt.Errorf("kafka.delay.FailAck taskJson is empty")
	}

	return retry.Do(func() error {
		_, err := client.ScriptRun(releaseScript,
			[]string{
				fmtQueueKey(timestamp, delayQueueName),
				fmtQueueKey(timestamp, reservedQueueName),
			}, []string{
				taskJson,
				cast.ToString(timestamp),
			})
		// 监控qps 耗时 失败次数
		metrics.Add(stat.Task{
			Duration: utils.Since(startTime),
			Drop:     true,
		})
		return err
	}, retry.Attempts(2), retry.Delay(100*time.Millisecond))
}

func fmtQueueKey(timestamp int64, queueType string) string {
	d := int(timestamp % int64(queueKeyBucketNum))
	return fmt.Sprintf(queueKey, queuePrefix, d, queueType)
}
