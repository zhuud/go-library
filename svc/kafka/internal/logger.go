package internal

import (
	"strings"

	"github.com/zeromicro/go-zero/core/logx"
)

// kafkaLogger 是 logx 链式调用的结果接口，用于缓存带字段的 logger，避免重复创建
type kafkaLogger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
	Slowf(format string, args ...any)
}

// writerLogger 是使用 logx 实现的 kafka.Logger，支持按 topic 区分
type writerLogger struct {
	logger kafkaLogger
}

func newWriterLogger(topic string) *writerLogger {
	return &writerLogger{
		logger: logx.WithCallerSkip(1).
			WithFields(logx.Field("component", "kafka.writer"), logx.Field("topic", topic)),
	}
}

func (l *writerLogger) Printf(msg string, args ...any) {
	if shouldFilterLog(msg) {
		return
	}
	l.logger.Infof(msg, args...)
}

func (l *writerLogger) Slowf(format string, args ...any) {
	l.logger.Slowf(format, args...)
}

// writerErrorLogger 是使用 logx 实现的 kafka.Logger，支持按 topic 区分
type writerErrorLogger struct {
	logger kafkaLogger
}

func newWriterErrorLogger(topic string) *writerErrorLogger {
	return &writerErrorLogger{
		logger: logx.WithCallerSkip(1).
			WithFields(logx.Field("component", "kafka.writer"), logx.Field("topic", topic)),
	}
}

func (l *writerErrorLogger) Printf(msg string, args ...any) {
	l.logger.Errorf(msg, args...)
}

func (l *writerErrorLogger) Slowf(format string, args ...any) {
	l.logger.Slowf(format, args...)
}

// readerLogger 是使用 logx 实现的 kafka.Logger，支持按 topic 区分
type readerLogger struct {
	logger kafkaLogger
}

func newReaderLogger(group string) *readerLogger {
	return &readerLogger{
		logger: logx.WithCallerSkip(1).
			WithFields(logx.Field("component", "kafka.reader"), logx.Field("group", group)),
	}
}

func (l *readerLogger) Printf(msg string, args ...any) {
	// 过滤某些日志，不打印 eg no messages received from kafka within the allocated time
	// the kafka reader for partition 3 of 79029 is seeking to offset 2208925
	if shouldFilterLog(msg) {
		return
	}
	l.logger.Infof(msg, args...)
}

func (l *readerLogger) Slowf(format string, args ...any) {
	l.logger.Slowf(format, args...)
}

// readerErrorLogger 是使用 logx 实现的 kafka.Logger，支持按 topic 区分
type readerErrorLogger struct {
	logger kafkaLogger
}

func newReaderErrorLogger(group string) *readerErrorLogger {
	return &readerErrorLogger{
		logger: logx.WithCallerSkip(1).
			WithFields(logx.Field("component", "kafka.reader"), logx.Field("group", group)),
	}
}

func (l *readerErrorLogger) Printf(msg string, args ...any) {
	l.logger.Errorf(msg, args...)
}

func (l *readerErrorLogger) Slowf(format string, args ...any) {
	l.logger.Slowf(format, args...)
}

// delayLoggerImpl 是使用 logx 实现的 kafka.delay 模块的 logger
type delayLogger struct {
	logger kafkaLogger
}

func newDelayLogger() *delayLogger {
	return &delayLogger{
		logger: logx.WithCallerSkip(1).
			WithFields(logx.Field("component", "kafka.delay")),
	}
}

func (l *delayLogger) Errorf(format string, args ...any) {
	l.logger.Errorf(format, args...)
}

func (l *delayLogger) Infof(format string, args ...any) {
	l.logger.Infof(format, args...)
}

// shouldFilterLog 判断是否应该过滤该日志
// 返回 true 表示应该过滤（不打印），false 表示正常打印
func shouldFilterLog(msg string) bool {
	// 转换为小写进行匹配，提高匹配的容错性
	lowerMsg := strings.ToLower(msg)

	// 定义需要过滤的关键词列表（已转换为小写）
	filterKeywords := []string{
		"no messages received",
		"is seeking to offset",
		"committed offsets for group",
		// error中 the kafka reader got an unknown error reading partition 1 of 79029 at offset 2237548: read tcp ->: i/o timeout 一般是因为maxwait时间太短 时间内没有收到数据 可以调整大一点
		// i/o timeout 就会 重新initializing kafka reader
		"initializing kafka reader",
		// writing 1 messages to 79034 (partition: 0)
		"writing",
	}

	// 检查消息是否包含任何需要过滤的关键词
	for _, keyword := range filterKeywords {
		if strings.Contains(lowerMsg, keyword) {
			return true
		}
	}

	return false
}
