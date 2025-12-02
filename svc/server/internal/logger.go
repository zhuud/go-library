package internal

import "github.com/zeromicro/go-zero/core/logx"

// Logger 是 logx 链式调用的结果接口，用于缓存带字段的 logger，避免重复创建
type Logger interface {
	Errorf(format string, args ...any)
	Infof(format string, args ...any)
}

// NewServerLogger 创建一个新的 server logger 实例
func NewServerLogger() Logger {
	return logx.WithCallerSkip(1).
		WithFields(logx.Field("component", "server"))
}
