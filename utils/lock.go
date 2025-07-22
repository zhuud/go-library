package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// MutexLock 分布式可重入锁
// 推荐通过 AcquireMutexLock 获取
type MutexLock struct {
	Key     string
	Value   string        // 唯一标识（用于可重入和安全释放）
	Timeout int           // 秒
	stopCh  chan struct{} // 自动续期控制
}

// AcquireMutexLock 获取分布式锁，支持自动释放
// 返回锁对象，若加锁失败（已被占用）返回 (nil, nil)
func AcquireMutexLock(ctx context.Context, r *redis.Redis, key string, seconds int) (*MutexLock, error) {
	lockValue := GenUniqId()
	ok, err := r.SetnxExCtx(ctx, key, lockValue, seconds)
	if err != nil {
		return nil, fmt.Errorf("utils.AcquireMutexLock.SetnxExCtx error %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("utils.AcquireMutexLock.SetnxExCtx failed key %s", key)
	}

	return &MutexLock{Key: key, Value: lockValue, Timeout: seconds}, nil
}

// Release 释放分布式锁（仅释放自己持有的锁）
func (l *MutexLock) Release(ctx context.Context, rdb *redis.Redis) error {
	script := `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end`
	res, err := rdb.EvalCtx(ctx, script, []string{l.Key}, l.Value)
	if err != nil {
		return fmt.Errorf("utils.mutexLock.Release.EvalCtx error %w", err)
	}
	if res == int64(0) {
		return fmt.Errorf("utils.mutexLock.Release: lock not held or already released key %s", l.Key)
	}

	return nil
}

// Extend 延长锁的过期时间（可重入场景）
func (l *MutexLock) Extend(ctx context.Context, rdb *redis.Redis, additional int) error {
	script := `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("expire", KEYS[1], ARGV[2])
else
	return 0
end`
	res, err := rdb.EvalCtx(ctx, script, []string{l.Key}, l.Value, additional)
	if err != nil {
		return fmt.Errorf("utils.mutexLock.Extend.EvalCtx error: %w", err)
	}
	if res == int64(0) {
		return fmt.Errorf("utils.mutexLock.Extend: lock not held cannot extend key %s", l.Key)
	}

	return nil
}

// StartAutoExtend 启动自动续期，interval建议小于Timeout一半
func (l *MutexLock) StartAutoExtend(ctx context.Context, rdb *redis.Redis, interval time.Duration) {
	if l == nil || l.stopCh != nil {
		return // 已经启动或未加锁
	}
	l.stopCh = make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logx.WithContext(ctx).Errorf("utils.mutexLock.AutoExtend panic: %v, key=%s", r, l.Key)
			}
		}()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := l.Extend(ctx, rdb, l.Timeout); err != nil {
					logx.WithContext(ctx).Errorf("utils.mutexLock.AutoExtend failed: %v, key=%s", err, l.Key)
				}
			case <-l.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopAutoExtend 停止自动续期
func (l *MutexLock) StopAutoExtend() {
	if l == nil || l.stopCh == nil {
		return
	}
	close(l.stopCh)
	l.stopCh = nil
}
