package fasthttp

import (
	"context"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/zeromicro/go-zero/core/breaker"
)

// 默认配置常量
const (
	// 超时配置
	// ReadTimeout 读超时时间，根据业务接口响应时间调整
	// 如果接口平均响应时间 < 1秒，可以设置为 10秒
	// 如果接口平均响应时间 1-3秒，建议设置为 15-20秒
	DefaultReadTimeout = 20 * time.Second
	// WriteTimeout 写超时时间，通常写操作较快，5-10秒足够
	DefaultWriteTimeout = 10 * time.Second
	// MaxConnWaitTimeout 连接用完后等待连接时间
	DefaultMaxConnWaitTimeout = 5 * time.Second

	// 并发控制
	// Concurrency 最大并发数
	DefaultConcurrency = 5 * 1024
	// MaxConnsPerHost 客户端可以与每个主机建立的最大并发连接数
	DefaultMaxConnsPerHost = 512

	// 连接复用
	// MaxIdleConnDuration 空闲连接持续时间
	// ⚠️ 关键配置：必须短于服务器端的连接超时时间
	// 建议设置为服务器端超时时间的 50-70%
	// 如果服务器端超时 30秒，设置为 15-20秒
	// 如果服务器端超时 60秒，设置为 30-40秒
	// 如果服务器端超时 15秒，设置为 8-10秒
	DefaultMaxIdleConnDuration = 10 * time.Second
	// MaxConnDuration 连接最大使用时间，超过此时间后连接会被关闭
	// 建议设置为 3-5分钟，避免使用过久的连接
	DefaultMaxConnDuration = 3 * time.Minute

	// DNSCacheDuration 缓存解析的 TCP 地址的持续时间
	DefaultDNSCacheDuration = 1 * time.Minute
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
		// ⚠️ 必须短于服务器端的连接超时时间，建议设置为服务器端超时时间的 50-70%
		MaxIdleConnDuration time.Duration
		// MaxConnsPerHost 客户端可以与每个主机建立的最大并发连接数
		MaxConnsPerHost int
		// Concurrency 最大并发数，0 表示无限制
		Concurrency int
		// DNSCacheDuration 缓存解析的 TCP 地址的持续时间
		DNSCacheDuration time.Duration
		// MaxConnDuration 连接最大使用时间，超过此时间后连接会被关闭
		// 建议设置为 3-5分钟，避免使用过久的连接
		MaxConnDuration time.Duration
		// Breaker 断路器（可选），如果为 nil 则不启用熔断
		Breaker breaker.Breaker
	}

	// OptionFunc 配置函数
	OptionFunc func(config *Conf)

	// RequestConfig 请求配置
	RequestConf struct {
		// Context 请求上下文
		Context context.Context
		// Retry 重试配置
		Retry *retryConf
	}

	// retryConf 重试配置（内部类型，不导出）
	retryConf struct {
		// MaxAttempts 最大重试次数
		MaxAttempts uint
		// InitialDelay 初始延迟时间
		InitialDelay time.Duration
		// MaxDelay 最大延迟时间
		MaxDelay time.Duration
		// DelayType 延迟类型
		DelayType retry.DelayTypeFunc
	}
	// RequestOptionFunc 请求配置函数
	RequestOptionFunc func(config *RequestConf)
)
