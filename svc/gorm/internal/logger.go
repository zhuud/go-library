package internal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// ===== 业务日志 Logger（用于 BaseDao 错误日志）=====

// GormLogger 是 logx 链式调用的结果接口，用于缓存带字段的 logger，避免重复创建
type GormLogger interface {
	Errorf(format string, args ...any)
}

// NewGormLogger 创建一个新的 gorm logger，使用 component 字段标识
func NewGormLogger(dbName, tableName string) GormLogger {
	return logx.WithCallerSkip(1).
		WithFields(logx.Field("component", "gorm")).
		WithFields(logx.Field("dbName", dbName)).
		WithFields(logx.Field("tableName", tableName))
}

// ===== GORM 框架 Logger（实现 gormlogger.Interface）=====

// FrameworkLogger GORM 框架日志记录器，实现 gormlogger.Interface
// TODO 实现 链路追踪
type FrameworkLogger struct {
	level gormlogger.LogLevel
}

// LogMode 设置日志级别
func (l *FrameworkLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	l.level = level
	return l
}

// Info 记录信息日志
func (l *FrameworkLogger) Info(ctx context.Context, s string, i ...interface{}) {
	// TODO: 实现信息日志记录
}

// Warn 记录警告日志
func (l *FrameworkLogger) Warn(ctx context.Context, s string, i ...interface{}) {
	// TODO: 实现警告日志记录
}

// Error 记录错误日志
func (l *FrameworkLogger) Error(ctx context.Context, s string, i ...interface{}) {
	// TODO: 实现错误日志记录
}

// Trace 记录 SQL 追踪日志
func (l *FrameworkLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	elapsed := time.Since(begin)

	switch {
	case err != nil && !errors.Is(err, gorm.ErrRecordNotFound):
		sql, rows := fc()
		log.Println(sql, rows)
	case elapsed > slowQueryThreshold && slowQueryThreshold != 0:
		sql, rows := fc()
		slowLog := fmt.Sprintf("SLOW SQL >= %v", slowQueryThreshold)
		log.Println(sql, rows, slowLog)
	case l.level == gormlogger.Info:
		sql, rows := fc()
		log.Println(sql, rows)
	}
}

// callSkip 计算调用栈跳过层数
func callSkip() int {
	// the second caller usually from gorm internal, so set i start from 2
	for i := 2; i < 15; i++ {
		_, file, _, ok := runtime.Caller(i)
		if ok && !strings.Contains(file, "/gorm.io/") {
			return i - 1
		}
	}
	return 0
}
