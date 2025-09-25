package fasthttp

import (
	"time"

	"github.com/valyala/fasthttp"
)

// Client FastHTTP客户端封装
type Client struct {
	*fasthttp.Client
}

func NewClient(conf ClientConf) *Client {
	// 设置默认值
	readTimeout := DefaultReadTimeout
	if conf.ReadTimeout > 0 {
		readTimeout = time.Duration(conf.ReadTimeout) * time.Second
	}

	writeTimeout := DefaultWriteTimeout
	if conf.WriteTimeout > 0 {
		writeTimeout = time.Duration(conf.WriteTimeout) * time.Second
	}

	maxConnWaitTimeout := DefaultMaxConnWaitTimeout
	if conf.MaxConnWaitTimeout > 0 {
		maxConnWaitTimeout = time.Duration(conf.MaxConnWaitTimeout) * time.Second
	}

	concurrency := DefaultConcurrency
	if conf.Concurrency > 0 {
		concurrency = conf.Concurrency
	}

	dnsCacheDuration := DefaultDNSCacheDuration
	if conf.DNSCacheDuration > 0 {
		dnsCacheDuration = time.Duration(conf.DNSCacheDuration) * time.Second
	}

	maxConnsPerHost := DefaultMaxConnsPerHost
	if conf.MaxConnsPerHost > 0 {
		maxConnsPerHost = conf.MaxConnsPerHost
	}

	maxIdleConnDuration := time.Duration(conf.MaxIdleConnDuration) * time.Second
	if conf.MaxIdleConnDuration <= 0 {
		maxIdleConnDuration = 10 * time.Minute
	}

	return &Client{
		&fasthttp.Client{
			// 读超时时间,不设置read超时,可能会造成连接复用失效
			ReadTimeout: readTimeout,
			// 写超时时间
			WriteTimeout: writeTimeout,
			// 链接用完后等待链接时间
			MaxConnWaitTimeout: maxConnWaitTimeout,
			MaxConnsPerHost:    maxConnsPerHost,
			// 空闲连接持续时间
			MaxIdleConnDuration: maxIdleConnDuration,
			Dial: (&fasthttp.TCPDialer{
				// 最大并发数，0表示无限制
				Concurrency: concurrency,
				// 缓存解析的 TCP 地址的持续时间
				DNSCacheDuration: dnsCacheDuration,
			}).Dial,
		},
	}
}
