package fasthttp

import (
	"time"

	"github.com/avast/retry-go/v4"
)

// 默认配置常量
const (
	DefaultReadTimeout         = 5 * time.Second
	DefaultWriteTimeout        = 5 * time.Second
	DefaultMaxConnWaitTimeout  = 5 * time.Second
	DefaultConcurrency         = 256 * 1024
	DefaultDNSCacheDuration    = 10 * time.Minute
	DefaultMaxConnsPerHost     = 512
	DefaultMaxIdleConnDuration = 10 * time.Minute
)

type (
	// Conf 客户端配置
	Conf struct {
		// ReadTimeout 读超时时间，不设置 read 超时可能会造成连接复用失效
		ReadTimeout time.Duration
		// WriteTimeout 写超时时间
		WriteTimeout time.Duration
		// MaxConnWaitTimeout 连接用完后等待连接时间
		MaxConnWaitTimeout time.Duration
		// MaxIdleConnDuration 空闲连接持续时间
		MaxIdleConnDuration time.Duration
		// MaxConnsPerHost 客户端可以与每个主机建立的最大并发连接数
		MaxConnsPerHost int
		// Concurrency 最大并发数，0 表示无限制
		Concurrency int
		// DNSCacheDuration 缓存解析的 TCP 地址的持续时间
		DNSCacheDuration time.Duration
	}

	// OptionFunc 配置函数
	OptionFunc func(config *Conf)

	// RetryConf 重试配置
	RetryConf struct {
		// MaxAttempts 最大重试次数
		MaxAttempts uint
		// InitialDelay 初始延迟时间
		InitialDelay time.Duration
		// MaxDelay 最大延迟时间
		MaxDelay time.Duration
		// DelayType 延迟类型
		DelayType retry.DelayTypeFunc
	}
)
