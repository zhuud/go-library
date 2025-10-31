package utils

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

var (
	onceTicketKey    = "lib:once:ticket:string:%s:%s"
	ErrTicketInvalid = fmt.Errorf("utils.Ticket ticket invalid or already used")
)

// SetTicket 一次性ticket通用
func SetTicket(ctx context.Context, r *redis.Redis, from, biz, ticket string, expireSec int) (string, error) {
	_, err := r.SetnxExCtx(ctx, GetCacheKey(onceTicketKey, from, biz), ticket, expireSec)
	if err != nil {
		return ticket, fmt.Errorf("utils.SetTicket.SetnxEx error: %w", err)
	}
	return ticket, nil
}

// CheckTicket 校验并删除一次性ticket，原子操作，幂等安全。
// 返回 nil 表示校验通过，ErrTicketInvalid 表示无效或已用过，其它error为系统异常。
func CheckTicket(ctx context.Context, r *redis.Redis, from, biz, ticket string) error {
	key := GetCacheKey(onceTicketKey, from, biz)
	script := `
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end`
	res, err := r.EvalCtx(ctx, script, []string{key}, ticket)
	if err != nil {
		return fmt.Errorf("utils.CheckTicket.EvalCtx error: %w", err)
	}
	if res == int64(1) {
		return nil
	}
	return ErrTicketInvalid
}

type FrequencyOption struct {
	Key    string `json:"key"`
	Second int    `json:"second"`
	Cnt    int    `json:"int"`
	Msg    string `json:"msg"`
}

// CheckFreq 频次校验通用（支持高并发，Lua原子操作）
// “降级”，只拦截频控，可以用 if !ok { return err }
// “不降级”，redis异常或者拦截频控，可以用 if err != nil { return err }
func CheckFreq(ctx context.Context, r *redis.Redis, optionList []FrequencyOption) (bool, error) {
	if len(optionList) == 0 {
		return true, nil
	}
	for _, option := range optionList {
		ok, err := checkFreq(ctx, r, option.Key, option.Cnt)
		if err != nil {
			// 错误降级，不影响主流程
			return true, err
		}
		if !ok {
			return false, fmt.Errorf(option.Msg)
		}
	}
	return true, nil
}

// SetFreq 频次自增+首次设置过期（支持高并发，Lua原子操作）
func SetFreq(ctx context.Context, r *redis.Redis, optionList []FrequencyOption) (bool, error) {
	if len(optionList) == 0 {
		return true, nil
	}
	for _, option := range optionList {
		_, err := setFreq(ctx, r, option.Key, option.Second)
		if err != nil {
			// 错误降级，不影响主流程
			return true, err
		}
	}
	return true, nil
}

// checkFreq 频次校验（只读判断，不自增，不设置过期）
func checkFreq(ctx context.Context, r *redis.Redis, key string, maxCnt int) (bool, error) {
	script := `
local cnt = redis.call('get', KEYS[1])
if not cnt then
    return 1
end
if tonumber(cnt) >= tonumber(ARGV[1]) then
    return 0
end
return 1
`
	res, err := r.EvalCtx(ctx, script, []string{key}, maxCnt)
	if err != nil {
		return false, fmt.Errorf("utils.checkFreq.EvalCtx error: %w", err)
	}
	return res == int64(1), nil
}

// setFreq 频次自增+首次设置过期（原子操作），返回当前计数
func setFreq(ctx context.Context, r *redis.Redis, key string, expireSec int) (int64, error) {
	script := `
local cnt = redis.call('incr', KEYS[1])
if cnt == 1 then
    redis.call('expire', KEYS[1], ARGV[1])
end
return cnt
`
	res, err := r.EvalCtx(ctx, script, []string{key}, expireSec)
	if err != nil {
		return 0, fmt.Errorf("utils.setFreq.EvalCtx error: %w", err)
	}
	return res.(int64), nil
}
