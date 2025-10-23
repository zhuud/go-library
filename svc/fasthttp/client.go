package fasthttp

import (
	"time"

	"github.com/valyala/fasthttp"
)

// Client FastHTTP 客户端封装
type Client struct {
	*fasthttp.Client
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
		&fasthttp.Client{
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
