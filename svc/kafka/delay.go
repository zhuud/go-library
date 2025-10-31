package kafka

import (
	"context"
	"encoding/json"
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
	"github.com/zhuud/go-library/svc/kafka/internal"
	"github.com/zhuud/go-library/utils"
)

// 关注错误
// kafka.delay consumer panic
// kafka.delay.sendAndAck Unmarshal data: %s, error: %v
// kafka.delay.sendAndAck.Push kafka data: %s, error: %v
// kafka.delay.sendAndAck.Ack data: %s, error: %v
// kafka.delay.FailAck max retry attempts exceeded, topic: %s, attempts: %d, max: %d

const (
	delayTrickInterval = time.Second
)

var (
	once   sync.Once
	hasSet int32
)

func DelaySetUp(redis *redis.Redis, batchSize int, prefix string) {
	DelaySetUpWithRetry(redis, batchSize, prefix, 0, 0)
}

// DelaySetUpWithRetry 设置延迟队列，支持配置重试参数
// maxAttempts: 最大重试次数，0 表示使用默认值 3
// retryDelay: 重试延迟时间，0 表示使用默认值 1 分钟
func DelaySetUpWithRetry(redis *redis.Redis, batchSize int, prefix string, maxAttempts int, retryDelay time.Duration) {
	once.Do(func() {
		internal.SetUpWithRetry(redis, batchSize, prefix, maxAttempts, retryDelay)
		go backgroundConsumeDelaySendKafka()
		atomic.AddInt32(&hasSet, 1)
	})
}

func PushDelay(ctx context.Context, topic string, data any, delayDuration time.Duration) error {
	if atomic.LoadInt32(&hasSet) == 0 {
		return fmt.Errorf("kafka.PushDelay must DelaySetUp before PushDelay")
	}
	return internal.Push(ctx, topic, data, delayDuration)
}

func backgroundConsumeDelaySendKafka() {
	log.Println("kafka.delay consumer start in the background")
	defer func() {
		if err := recover(); err != nil {
			errorLog(fmt.Sprintf("kafka.delay consumer panic: %v", err))
		}
		log.Println("kafka.delay consumer stop")
	}()

	// 创建可取消的 context
	ctx, cancel := context.WithCancel(context.Background())

	// 异步执行任务 1S或者100条一批次执行
	task := executors.NewBulkExecutor(func(tasks []any) {
		sendAndAck(cast.ToStringSlice(tasks))
	}, executors.WithBulkTasks(100))

	// NewBulkExecutor虽然注册Shutdown，但是资源链接也在Shutdown关闭，避免资源链接被关掉，注册WrapUp
	proc.AddWrapUpListener(func() {
		cancel()
		log.Println("kafka.delay consumer task.Flush")
		task.Flush()
	})

	ticker := time.NewTicker(delayTrickInterval)
	defer ticker.Stop()

	// 常驻 定时pop  数据被task异步执行
	for {
		select {
		case <-ctx.Done():
			log.Println("kafka.delay consumer exiting")
			return
		case <-ticker.C:
			taskJsonList := internal.Pop()
			for _, taskJson := range taskJsonList {
				logx.Infof("kafka.delay consumer task.Add data: %s", taskJson)
				_ = task.Add(taskJson)
			}
		}
	}
}

func sendAndAck(taskJsonList []string) {
	for _, taskJson := range taskJsonList {
		now := utils.Now()
		var delayData internal.DelayData
		err := json.Unmarshal([]byte(taskJson), &delayData)
		// 逻辑上不会有这种情况
		if err != nil {
			errorLog(fmt.Sprintf("kafka.delay.sendAndAck Unmarshal data: %s, error: %v", taskJson, err))
			// 无法解析的数据直接跳过
			_ = internal.SuccessAck(0, taskJson, now)
			continue
		}

		// 逻辑上不会有这种情况
		// 校验必填字段
		if len(delayData.Topic) == 0 {
			errorLog(fmt.Sprintf("kafka.delay.sendAndAck invalid data: %s", taskJson))
			// 无效数据直接删除，避免堆积
			_ = internal.SuccessAck(delayData.Timestamp, taskJson, now)
			continue
		}

		// TODO 链路追踪
		ctx := context.Background()
		logx.Infof("kafka.delay.sendAndAck forward delay message, topic: %s, timestamp: %d", delayData.Topic, time.Now().Unix())
		err = Push(ctx, delayData.Topic, delayData.Data)
		if err != nil {
			errorLog(fmt.Sprintf("kafka.delay.sendAndAck.Push kafka data: %s, error: %v", taskJson, err))
			err = internal.FailAck(delayData.Timestamp, taskJson, now)
		} else {
			err = internal.SuccessAck(delayData.Timestamp, taskJson, now)
		}
		if err != nil {
			errorLog(fmt.Sprintf("kafka.delay.sendAndAck.Ack data: %s, error: %v", taskJson, err))
		}
	}
}

func errorLog(msg string) {
	logx.Error(msg)
}
