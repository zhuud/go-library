package internal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// TODO 实现 链路追踪
type Logger struct {
	level gormlogger.LogLevel
}

func (l *Logger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	l.level = level
	return l
}

func (l *Logger) Info(ctx context.Context, s string, i ...interface{}) {

}

func (l *Logger) Warn(ctx context.Context, s string, i ...interface{}) {

}

func (l *Logger) Error(ctx context.Context, s string, i ...interface{}) {

}

func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
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
