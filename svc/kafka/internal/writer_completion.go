package internal

import (
	"context"
	"encoding/json"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/stat"
)

// newDefaultCompletionCallback 创建默认 completion callback，保留失败和慢日志，避免成功投递重复刷屏。
func newDefaultCompletionCallback(eventLogger *eventLogger, metrics *stat.Metrics) func(messages []kafka.Message, err error) {
	return func(messages []kafka.Message, err error) {
		defer func() {
			if r := recover(); r != nil {
				eventLogger.Errorf(context.Background(), "push completion panic messages:%+v, panic:%+v \nstack:%s", messages, r, string(debug.Stack()))
			}
		}()
		now := time.Now()

		for _, msg := range messages {
			duration := calculateDuration(msg, now)
			if duration >= 0 {
				metrics.Add(stat.Task{
					Duration: duration,
					Drop:     err != nil,
				})
			}
			ctx := contextFromMessage(msg)
			if duration >= 0 {
				if duration > defaultWriterSlowThreshold {
					eventLogger.Slowf(ctx, "delivery slow partition:%d, offset:%d, key:%s, value:%s, duration:%d", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value), duration)
				}
			}
			if err != nil {
				eventLogger.Errorf(ctx, "delivery failed partition:%d, offset:%d, key:%s, value:%s, error:%v", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value), err)
			}
		}
	}
}

// calculateDuration 优先从 key 解析纳秒时间戳；失败时回退从 value 的 timestamp（Unix秒）解析。
func calculateDuration(msg kafka.Message, now time.Time) time.Duration {
	if ts, err := strconv.ParseInt(string(msg.Key), 10, 64); err == nil {
		if duration := validateDuration(now.Sub(time.Unix(0, ts))); duration >= 0 {
			return duration
		}
	}

	var payload struct {
		Timestamp int64 `json:"timestamp"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil || payload.Timestamp <= 0 {
		return -1
	}
	return validateDuration(now.Sub(time.Unix(payload.Timestamp, 0)))
}

func validateDuration(duration time.Duration) time.Duration {
	// 如果耗时异常（负数或过大），返回 -1
	if duration < 0 || duration > 24*time.Hour {
		return -1
	}
	return duration
}
