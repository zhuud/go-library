package internal

import "github.com/zeromicro/go-zero/core/logx"

type delayLogger struct {
	logger interface {
		Infof(format string, args ...any)
		Errorf(format string, args ...any)
	}
}

func newDelayLogger(queueName string) *delayLogger {
	return &delayLogger{
		logger: logx.WithCallerSkip(1).
			WithFields(logx.Field("component", queueName)),
	}
}

func (l *delayLogger) Errorf(format string, args ...any) {
	l.logger.Errorf(format, args...)
}

func (l *delayLogger) Infof(format string, args ...any) {
	l.logger.Infof(format, args...)
}
