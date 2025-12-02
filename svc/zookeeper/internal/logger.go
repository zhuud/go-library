package internal

import (
	"github.com/go-zookeeper/zk"
	"github.com/zeromicro/go-zero/core/logx"
)

// Logger 是 logx 链式调用的结果接口，用于缓存带字段的 logger，避免重复创建
type Logger interface {
	Infof(format string, args ...any)
}

var _ zk.Logger = (*ZookeeperLogger)(nil)

// ZookeeperLogger 同时实现 Logger 和 zk.Logger 接口
type ZookeeperLogger struct {
	logger Logger
}

// NewZookeeperLogger 创建一个新的 zookeeper logger 实例
func NewZookeeperLogger() *ZookeeperLogger {
	return &ZookeeperLogger{
		logger: logx.WithCallerSkip(1).
			WithFields(logx.Field("component", "zookeeper")),
	}
}

// Infof 实现 Logger 接口
func (l *ZookeeperLogger) Infof(format string, args ...any) {
	l.logger.Infof(format, args...)
}

// Printf 实现 zk.Logger 接口
func (l *ZookeeperLogger) Printf(format string, args ...any) {
	l.logger.Infof("%s: %v", format, args)
}
