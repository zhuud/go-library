package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zhuud/go-library/svc/alarm"
	"github.com/zhuud/go-library/svc/kafka/internal"
)

const (
	delayTrickInterval = time.Second
)

var (
	once   sync.Once
	hasSet int32
)

func DelaySetUp(redis *redis.Redis, batchSize int, prefix string) {
	once.Do(func() {
		internal.SetUp(redis, batchSize, prefix)
		threading.GoSafe(backgroundConsumeDelaySendKafka)
		atomic.AddInt32(&hasSet, 1)
	})
}

func PushDelay(ctx context.Context, topic string, data any, delayDuration time.Duration) error {
	if hasSet == 0 {
		return errors.New("must DelaySetUp before PushDelay")
	}
	return internal.Push(ctx, topic, data, delayDuration)
}

func backgroundConsumeDelaySendKafka() {
	log.Println("kafka delay consumer start in the background")
	defer func() {
		if err := recover(); err != nil {
			errorLog(fmt.Sprintf("kafka delay consumer error: %v", err))
		}
		log.Println("kafka delay consumer stop")
	}()

	// 异步执行任务
	task := executors.NewBulkExecutor(func(tasks []any) {
		sendAndAck(cast.ToStringSlice(tasks))
	}, executors.WithBulkTasks(1))

	// NewBulkExecutor虽然注册Shutdown，但是资源链接也在Shutdown关闭，避免资源链接被关掉，注册WrapUp
	defer proc.AddWrapUpListener(func() {
		log.Println("kafka delay consumer task.Flush")
		task.Flush()
	})

	ticker := time.NewTicker(delayTrickInterval)
	defer ticker.Stop()
	// 常驻 定时pop  数据被task异步执行
	for {
		select {
		case <-ticker.C:
			jsonDataList := internal.Pop()
			for _, jsonData := range jsonDataList {
				logx.Infof("kafka delay consumer task.Add data:%s", jsonData)
				_ = task.Add(jsonData)
				// TODO 测试
				// time.Sleep(100 * time.Minute)
				// {"@timestamp":"2024-10-29T20:57:20.559+08:00","caller":"kq/queue.go:192","content":"EOF","level":"error"}
			}
		}
	}
}

func sendAndAck(jsonDataList []string) {
	for _, jsonData := range jsonDataList {
		var delayData *internal.DelayData
		err := json.Unmarshal([]byte(jsonData), &delayData)
		if err != nil {
			errorLog(fmt.Sprintf("kafka.sendAndAck Unmarshal data error: %v", err))
			continue
		}
		if delayData == nil || len(delayData.Topic) == 0 {
			errorLog(fmt.Sprintf("kafka.sendAndAck empty data error, data: %s", jsonData))
			continue
		}

		ctx, ok := delayData.Ctx.(context.Context)
		if !ok {
			ctx = context.Background()
		}
		logx.Infof("kafka.sendAndAck forward delay message, data:%s", jsonData)
		err = Push(ctx, delayData.Topic, delayData.Data)
		if err != nil {
			errorLog(fmt.Sprintf("kafka.sendAndAck Push kafka data: %s, error: %v", jsonData, err))

			err = internal.FailAck(jsonData)
		} else {
			err = internal.SuccessAck(jsonData)
		}
		if err != nil {
			errorLog(fmt.Sprintf("kafka.sendAndAck Ack data: %s, error: %v", jsonData, err))
		}
	}
}

func errorLog(msg string) {
	logx.Error(msg)
	_ = alarm.Send(alarm.LarkMessage{
		Content: msg,
	})
}
