package utils

import (
	"errors"
	"fmt"

	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

var onceTicketKey = "lib:once:ticket:string:%s:%s"

type FrequencyOption struct {
	Key    string `json:"key"`
	Second int    `json:"second"`
	Cnt    int    `json:"int"`
	Msg    string `json:"msg"`
}

func SetOnceTicketKey(key string) {
	onceTicketKey = key
}

// SetTicket 一次性ticket通用
func SetTicket(r *redis.Redis, from string, biz string, second int, ticket string) (string, error) {
	_, err := r.SetnxEx(GetCacheKey(onceTicketKey, from, biz), ticket, second)
	return ticket, err
}

func CheckTicket(r *redis.Redis, from string, biz string, ticket string) (bool, error) {
	key := GetCacheKey(onceTicketKey, from, biz)
	val, err := r.Get(key)
	if err != nil {
		return false, err
	}
	if len(val) == 0 {
		return false, errors.New("CheckTicket failed")
	}
	if ticket != `` && val != ticket {
		return false, errors.New("CheckTicket error")
	}
	_, err = r.Del(key)
	if err != nil {
		return false, errors.New(fmt.Sprintf("CheckTicket.DelCtx error %v", err))
	}

	return true, nil
}

// CheckFreq 频次校验通用  业务校验使用 高并发不支持
func CheckFreq(r *redis.Redis, optionList []FrequencyOption) (bool, error) {
	if len(optionList) == 0 {
		return false, errors.New("empty option")
	}
	for _, option := range optionList {
		flag, err := checkFreq(r, option.Key, option.Cnt)
		if err != nil {
			return false, err
		}
		if flag == false {
			return false, errors.New(option.Msg)
		}
	}
	return true, nil
}

func SetFreq(r *redis.Redis, optionList []FrequencyOption) (bool, error) {
	if len(optionList) == 0 {
		return false, errors.New("empty option")
	}
	for _, option := range optionList {
		_, err := setFreq(r, option.Key, option.Second)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// TODO lua 脚本原子操作
func checkFreq(r *redis.Redis, key string, cnt int) (bool, error) {
	val, err := r.Get(key)
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	if cast.ToInt(val) >= cnt {
		logx.Infof("CheckFreq.Get- key:%s, cnt:%s", key, val)
		return false, nil
	}
	return true, nil
}

// TODO lua 脚本原子操作
func setFreq(r *redis.Redis, key string, second int) (bool, error) {
	// 判断键是否存在
	exists, err := r.Exists(key)
	if err != nil {
		return false, err
	}

	if exists {
		// 键已存在，执行INCR命令自增值
		cnt, err := r.Incr(key)
		if err != nil {
			return false, err
		}
		logx.Infof("SetFreq.Incr- key:%s, cnt:%d", key, cnt)
	} else {
		// 键不存在，执行SET命令设置初始值
		err := r.Setex(key, "1", second)
		if err != nil {
			return false, err
		}
		logx.Info("SetFreq.Set- Initial Value Set")
	}

	return true, nil
}
