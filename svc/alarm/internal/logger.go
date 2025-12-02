package internal

import "github.com/zeromicro/go-zero/core/logx"

// AlarmLogger 是 logx 链式调用的结果接口，用于缓存带字段的 logger，避免重复创建
type AlarmLogger interface {
	Errorf(format string, args ...any)
}

// NewAlarmLogger 创建一个新的 alarm logger，使用 component 字段标识
func NewAlarmLogger() AlarmLogger {
	return logx.WithCallerSkip(1).
		WithFields(logx.Field("component", "alarm"))
}
