package internal

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	// 操作key要在同一节点上 {}, key要保存到那个节点上时,是根据'{}它里面的值来计算
	queueKey          = "{kafka:delay:queue}:%s:%d:%s"
	delayQueueName    = "delayed"
	reservedQueueName = "reserved"
)

var (
	client            *redis.Redis
	mu                sync.Mutex
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
		Ctx       any
		Topic     string `json:"topic"`
		Data      any    `json:"data"`
		Attempts  int    `json:"attempts"`
		Timestamp int64  `json:"timestamp"`
	}
)

func SetUp(redis *redis.Redis, batchSize int, prefix string) {
	client = redis
	queueGetBatchSize = batchSize
	queuePrefix = prefix
}

func Push(ctx context.Context, topic string, data any, delayDuration time.Duration) error {
	if client == nil {
		return errors.New("delay queue must setup before")
	}

	ts := time.Now().Local().Add(delayDuration).Unix()
	dd := DelayData{
		Ctx:       ctx,
		Topic:     topic,
		Data:      data,
		Timestamp: ts,
	}
	dj, err := json.Marshal(dd)
	if err != nil {
		return errors.New(fmt.Sprintf("delay.Push Marshal error: %v", err))
	}

	_, err = client.ZaddCtx(ctx, fmtQueueKey(ts, delayQueueName), ts, string(dj))
	if err != nil {
		return errors.New(fmt.Sprintf("delay.Push ZaddCtx error: %v", err))
	}

	return nil
}

func Pop() []string {
	mu.Lock()
	defer mu.Unlock()

	ts := time.Now().Local().Unix()

	list := make([]string, 0)
	if client == nil {
		logx.Error("delay.Pop client nil")
		return list
	}
	for i := 0; i < queueKeyBucketNum; i++ {

		data, err := client.ScriptRun(popScript,
			[]string{
				fmtQueueKey(int64(i), delayQueueName),
				fmtQueueKey(int64(i), reservedQueueName),
			}, []string{
				cast.ToString(ts),
				cast.ToString(queueGetBatchSize),
			})
		if err != nil {
			logx.Error(fmt.Sprintf("delay.Pop ScriptRun error: %v", err))
			continue
		}
		item := cast.ToStringSlice(data)
		if len(item) > 0 {
			list = append(list, item...)
		}
	}

	return list
}

func SuccessAck(value string) error {
	if len(value) == 0 {
		return errors.New("delay.SuccessAck value is empty")
	}
	var data DelayData
	_ = json.Unmarshal([]byte(value), &data)

	_, err := client.Zrem(fmtQueueKey(data.Timestamp, reservedQueueName), value)
	return err
}

func FailAck(value string) error {
	if len(value) == 0 {
		return errors.New("delay.SuccessAck value is empty")
	}
	var data DelayData
	_ = json.Unmarshal([]byte(value), &data)

	_, err := client.ScriptRun(releaseScript,
		[]string{
			fmtQueueKey(data.Timestamp, delayQueueName),
			fmtQueueKey(data.Timestamp, reservedQueueName),
		}, []string{
			value,
			cast.ToString(data.Timestamp),
		})

	return err
}

// TODO 监控 报警

func fmtQueueKey(timestamp int64, queueType string) string {
	d := int(timestamp % int64(queueKeyBucketNum))
	return fmt.Sprintf(queueKey, queuePrefix, d, queueType)
}
