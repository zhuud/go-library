package internal

import (
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
