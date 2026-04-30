package internal

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

const (
	defaultKafkaLogThrottleWindow = time.Minute
)

type kafkaLogger struct {
	logger  logx.Logger
	limiter *logLimiter
}

func newWriterErrorLogger(topic string) *kafkaLogger {
	return newKafkaLogger("kafka.writer", logx.Field("topic", topic))
}

func newReaderErrorLogger(topic, group string) *kafkaLogger {
	return newKafkaLogger("kafka.reader", logx.Field("topic", topic), logx.Field("group", group))
}

func newKafkaLogger(component string, fields ...logx.LogField) *kafkaLogger {
	fields = append([]logx.LogField{
		logx.Field("component", component),
	}, fields...)
	return &kafkaLogger{
		logger:  logx.WithCallerSkip(1).WithFields(fields...),
		limiter: newLogLimiter(defaultKafkaLogThrottleWindow),
	}
}

func (l *kafkaLogger) Printf(msg string, args ...any) {
	formatted := fmt.Sprintf(msg, args...)
	category := classifyKafkaLog(formatted)
	allowed, suppressed := l.limiter.Allow(category)
	if !allowed {
		return
	}
	fields := []logx.LogField{
		logx.Field("category", category),
		logx.Field("suppressed_count", suppressed),
	}
	l.logger.WithFields(fields...).Error(formatted)
}




type eventLogger struct {
	logger logx.Logger
}

func newWriterEventLogger(topic string) *eventLogger {
	return newEventLogger("kafka.writer", logx.Field("topic", topic))
}

func newReaderEventLogger(topic, group string) *eventLogger {
	return newEventLogger("kafka.reader", logx.Field("topic", topic), logx.Field("group", group))
}

func newEventLogger(component string, fields ...logx.LogField) *eventLogger {
	fields = append([]logx.LogField{
		logx.Field("component", component),
	}, fields...)
	return &eventLogger{
		logger: logx.WithCallerSkip(1).WithFields(fields...),
	}
}

func (l *eventLogger) Infof(ctx context.Context, format string, args ...any) {
	l.withContextFields(ctx).Infof(format, args...)
}

func (l *eventLogger) Errorf(ctx context.Context, format string, args ...any) {
	l.withContextFields(ctx).Errorf(format, args...)
}

func (l *eventLogger) Slowf(ctx context.Context, format string, args ...any) {
	l.withContextFields(ctx).Slowf(format, args...)
}

func (l *eventLogger) withContextFields(ctx context.Context) logx.Logger {
	return l.logger.WithContext(ctx)
}




type logLimiter struct {
	mu      sync.Mutex
	window  time.Duration
	records map[string]logLimitRecord
}

type logLimitRecord struct {
	last       time.Time
	suppressed int64
}

func newLogLimiter(window time.Duration) *logLimiter {
	return &logLimiter{
		window:  window,
		records: make(map[string]logLimitRecord),
	}
}

func (l *logLimiter) Allow(key string) (bool, int64) {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	record := l.records[key]
	if record.last.IsZero() || now.Sub(record.last) >= l.window {
		suppressed := record.suppressed
		l.records[key] = logLimitRecord{last: now}
		return true, suppressed
	}
	record.suppressed++
	l.records[key] = record
	return false, 0
}

func classifyKafkaLog(message string) string {
	lowerMsg := strings.ToLower(message)
	switch {
	case containsAny(lowerMsg,
		"i/o timeout",
		"read tcp ",
		"dial tcp ",
		"context deadline exceeded",
		"request timed out",
		"connection reset by peer",
		"broken pipe",
		"unexpected eof",
		"use of closed network connection",
		"network exception",
	):
		return "network"
	case containsAny(lowerMsg,
		"group coordinator",
		"rebalance",
		"join group",
		"sync group",
		"heartbeat",
		"not coordinator for group",
		"group coordinator not available",
		"group load in progress",
		"illegal generation",
		"unknown member id",
		"inconsistent group protocol",
		"member id required",
		"fenced instance id",
		"group id not found",
		"group subscribed to topic",
		"non empty group",
	):
		return "consumer_group"
	case containsAny(lowerMsg,
		"leader not available",
		"not leader for partition",
		"metadata",
		"unknown topic or partition",
		"broker not available",
		"replica not available",
		"not enough replicas",
		"stale controller epoch",
		"not controller",
		"unknown leader epoch",
		"fenced leader epoch",
		"eligible leader not available",
		"inconsistent topic id",
		"unknown topic id",
	):
		return "metadata"
	case containsAny(lowerMsg,
		"offset",
		"unstable offset commit",
		"invalid commit offset size",
		"offset metadata too large",
	):
		return "offset"
	case containsAny(lowerMsg,
		"sasl authentication failed",
		"unsupported sasl mechanism",
		"illegal sasl state",
		"topic authorization failed",
		"group authorization failed",
		"cluster authorization failed",
		"broker authorization failed",
		"transactional id authorization failed",
	):
		return "auth"
	default:
		return "unknown"
	}
}

func containsAny(s string, keywords ...string) bool {
	for _, keyword := range keywords {
		if strings.Contains(s, keyword) {
			return true
		}
	}
	return false
}
