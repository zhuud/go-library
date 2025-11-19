package fasthttp

import (
	"context"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/valyala/fasthttp"
	"github.com/zeromicro/go-zero/core/breaker"
)

// Client FastHTTP 客户端封装
type Client struct {
	*fasthttp.Client
	brk breaker.Breaker // 断路器（可选），如果为 nil 则不启用熔断
}

// New 创建一个新的 FastHTTP 客户端实例
func New(opts ...OptionFunc) *Client {
	// 默认配置
	config := &Conf{
		ReadTimeout:         DefaultReadTimeout,
		WriteTimeout:        DefaultWriteTimeout,
		MaxConnWaitTimeout:  DefaultMaxConnWaitTimeout,
		MaxIdleConnDuration: DefaultMaxIdleConnDuration,
		MaxConnsPerHost:     DefaultMaxConnsPerHost,
		Concurrency:         DefaultConcurrency,
		DNSCacheDuration:    DefaultDNSCacheDuration,
	}

	// 应用配置函数
	for _, opt := range opts {
		opt(config)
	}

	return &Client{
		Client: &fasthttp.Client{
			// 读超时时间，不设置 read 超时可能会造成连接复用失效
			ReadTimeout: config.ReadTimeout,
			// 写超时时间
			WriteTimeout: config.WriteTimeout,
			// 连接用完后等待连接时间
			MaxConnWaitTimeout: config.MaxConnWaitTimeout,
			// 每个主机的最大连接数
			MaxConnsPerHost: config.MaxConnsPerHost,
			// 空闲连接持续时间
			MaxIdleConnDuration: config.MaxIdleConnDuration,
			Dial: (&fasthttp.TCPDialer{
				// 最大并发数，0 表示无限制
				Concurrency: config.Concurrency,
				// 缓存解析的 TCP 地址的持续时间
				DNSCacheDuration: config.DNSCacheDuration,
			}).Dial,
		},
		brk: config.Breaker, // 使用配置中的 breaker，如果为 nil 则不启用熔断
	}
}

// ===== 配置选项函数 =====

// WithReadTimeout 设置读超时时间
func WithReadTimeout(timeout time.Duration) OptionFunc {
	return func(config *Conf) {
		if timeout > 0 {
			config.ReadTimeout = timeout
		}
	}
}

// WithWriteTimeout 设置写超时时间
func WithWriteTimeout(timeout time.Duration) OptionFunc {
	return func(config *Conf) {
		if timeout > 0 {
			config.WriteTimeout = timeout
		}
	}
}

// WithMaxConnWaitTimeout 设置连接等待超时时间
func WithMaxConnWaitTimeout(timeout time.Duration) OptionFunc {
	return func(config *Conf) {
		if timeout > 0 {
			config.MaxConnWaitTimeout = timeout
		}
	}
}

// WithMaxIdleConnDuration 设置空闲连接持续时间
func WithMaxIdleConnDuration(duration time.Duration) OptionFunc {
	return func(config *Conf) {
		if duration > 0 {
			config.MaxIdleConnDuration = duration
		}
	}
}

// WithMaxConnsPerHost 设置每个主机的最大连接数
func WithMaxConnsPerHost(maxConns int) OptionFunc {
	return func(config *Conf) {
		if maxConns > 0 {
			config.MaxConnsPerHost = maxConns
		}
	}
}

// WithConcurrency 设置最大并发数
func WithConcurrency(concurrency int) OptionFunc {
	return func(config *Conf) {
		if concurrency >= 0 {
			config.Concurrency = concurrency
		}
	}
}

// WithDNSCacheDuration 设置 DNS 缓存持续时间
func WithDNSCacheDuration(duration time.Duration) OptionFunc {
	return func(config *Conf) {
		if duration > 0 {
			config.DNSCacheDuration = duration
		}
	}
}

// WithBreaker 设置断路器（可选）
// 如果不设置，则不启用熔断功能
func WithBreaker(brk breaker.Breaker) OptionFunc {
	return func(config *Conf) {
		config.Breaker = brk
	}
}

// ===== 请求配置选项函数 =====

// WithContext 设置请求上下文
func WithContext(ctx context.Context) RequestOptionFunc {
	return func(config *RequestConf) {
		if ctx != nil {
			config.Context = ctx
		}
	}
}

// WithRetryAttempts 设置最大重试次数
func WithRetryAttempts(attempts uint) RequestOptionFunc {
	return func(config *RequestConf) {
		if config.Retry == nil {
			config.Retry = &retryConf{}
		}
		if attempts > 0 {
			config.Retry.MaxAttempts = attempts
		}
	}
}

// WithRetryInitialDelay 设置初始延迟时间
func WithRetryInitialDelay(delay time.Duration) RequestOptionFunc {
	return func(config *RequestConf) {
		if config.Retry == nil {
			config.Retry = &retryConf{}
		}
		if delay > 0 {
			config.Retry.InitialDelay = delay
		}
	}
}

// WithRetryMaxDelay 设置最大延迟时间
func WithRetryMaxDelay(delay time.Duration) RequestOptionFunc {
	return func(config *RequestConf) {
		if config.Retry == nil {
			config.Retry = &retryConf{}
		}
		if delay > 0 {
			config.Retry.MaxDelay = delay
		}
	}
}

// WithRetryDelayType 设置延迟类型
func WithRetryDelayType(delayType retry.DelayTypeFunc) RequestOptionFunc {
	return func(config *RequestConf) {
		if config.Retry == nil {
			config.Retry = &retryConf{}
		}
		if delayType != nil {
			config.Retry.DelayType = delayType
		}
	}
}
