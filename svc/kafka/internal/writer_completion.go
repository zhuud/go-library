package internal

import (
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/stat"
)

const (
	// defaultSlowThreshold 默认慢日志阈值，超过此耗时的消息将打印慢日志
	defaultWriterSlowThreshold = time.Second * 2
)

// newDefaultCompletionCallback 创建一个使用 writerLogger 和 writerErrorLogger 的 completion callback
// 接收已创建的 logger 实例，避免重复实例化
func newDefaultCompletionCallback(infoLogger *writerLogger, errorLogger *writerErrorLogger, metrics *stat.Metrics) func(messages []kafka.Message, err error) {
	return func(messages []kafka.Message, err error) {
		defer func() {
			if r := recover(); r != nil {
				errorLogger.Printf("kafka delivery panic: %v, messages: %+v", r, messages)
			}
		}()
		now := time.Now()
		if err != nil {
			for _, msg := range messages {
				// 从 key 解析纳秒时间戳并计算耗时
				if duration := calculateDurationFromKey(msg.Key, now); duration >= 0 {
					metrics.Add(stat.Task{
						Duration: duration,
						Drop:     true,
					})
					// 如果超过慢日志阈值，打印慢日志
					if duration > defaultWriterSlowThreshold {
						errorLogger.Slowf("kafka message delivered slow: duration=%d, topic=%s, partition=%d, offset=%d, key=%s, message=%s",
							duration, msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
					}
				}
				errorLogger.Printf("kafka message delivered failed: error=%v, topic=%s, partition=%d, offset=%d, key=%s, message=%s",
					err, msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			}
		} else {
			for _, msg := range messages {
				// 从 key 解析纳秒时间戳并计算耗时
				if duration := calculateDurationFromKey(msg.Key, now); duration >= 0 {
					metrics.Add(stat.Task{
						Duration: duration,
					})
					// 如果超过慢日志阈值，打印慢日志
					if duration > defaultWriterSlowThreshold {
						infoLogger.Slowf("kafka message delivered slow: duration=%d, topic=%s, partition=%d, offset=%d, key=%s, message=%s",
							duration, msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
					}
				}
				infoLogger.Printf("kafka message delivered: topic=%s, partition=%d, offset=%d, key=%s, message=%s",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			}
		}
	}
}

// calculateDurationFromKey 从 key 中解析纳秒时间戳并计算耗时
// key 是纳秒时间戳的字符串形式
// 返回耗时，如果解析失败返回 -1
func calculateDurationFromKey(key []byte, now time.Time) time.Duration {
	if len(key) == 0 {
		return -1
	}

	// 解析纳秒时间戳
	timestamp, err := strconv.ParseInt(string(key), 10, 64)
	if err != nil {
		return -1
	}

	// 计算耗时
	startTime := time.Unix(0, timestamp)
	duration := now.Sub(startTime)

	// 如果耗时异常（负数或过大），返回 -1
	if duration < 0 || duration > 24*time.Hour {
		return -1
	}

	return duration
}
