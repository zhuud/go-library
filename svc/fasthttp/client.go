package fasthttp

import (
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

type Client struct {
	*fasthttp.Client
}

var (
	client *Client
	mu     sync.RWMutex
)

func NewFastHttp(conf ClientConf) *Client {

	if client != nil {
		return client
	}

	mu.Lock()
	defer mu.Unlock()

	client = &Client{
		&fasthttp.Client{
			// 读超时时间,不设置read超时,可能会造成连接复用失效
			ReadTimeout: time.Duration(conf.ReadTimeout) * time.Second,
			// 写超时时间
			WriteTimeout: time.Duration(conf.WriteTimeout) * time.Second,
			// 链接用完后等待链接时间
			MaxConnWaitTimeout: time.Duration(conf.MaxConnWaitTimeout) * time.Second,
			Dial: (&fasthttp.TCPDialer{
				// 最大并发数，0表示无限制
				Concurrency: DefaultConcurrency,
				// 缓存解析的 TCP 地址的持续时间
				DNSCacheDuration: DefaultDNSCacheDuration,
			}).Dial,
		},
	}

	return client
}
