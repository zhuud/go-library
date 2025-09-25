package fasthttp

import (
	"github.com/avast/retry-go/v4"
	"time"
)

const (
	DefaultReadTimeout        = 5 * time.Second
	DefaultWriteTimeout       = 5 * time.Second
	DefaultMaxConnWaitTimeout = 5 * time.Second
	DefaultConcurrency        = 256 * 1024
	DefaultDNSCacheDuration   = 10 * time.Minute
	DefaultMaxConnsPerHost    = 512
)

type (
	ClientConf struct {
		// 读超时时间,不设置read超时,可能会造成连接复用失效
		ReadTimeout int `json:",optional,default=5"`
		// 写超时时间
		WriteTimeout int `json:",optional,default=5"`
		// 链接用完后等待链接时间
		MaxConnWaitTimeout int `json:",optional,default=5"`
		// 空闲的活动连接活跃时间
		MaxIdleConnDuration int `json:",optional,default=1024"`
		// 从请求中去掉User-Agent标头
		//NoDefaultUserAgentHeader bool `json:",optional,default=true"`
		// 头部按照原样传输 默认会根据标准化转化
		//DisableHeaderNamesNormalizing bool `json:",optional,default=false"`
		// 路径按原样传输 默认会根据标准化转化
		//DisablePathNormalizing bool `json:",optional,default=false"`
		// 客户端可以与每个主机建立的最大并发连接数
		MaxConnsPerHost int `json:",optional,default=512"`
		// 最大并发数，0表示无限制
		Concurrency int `json:",optional,default=262144"`
		// 通过 Dial* 函数缓存解析的 TCP 地址的持续时间
		DNSCacheDuration int `json:",optional,default=60"`
	}

	// RetryConfig 重试配置
	RetryConfig struct {
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
