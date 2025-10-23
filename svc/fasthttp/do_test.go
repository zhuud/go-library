package fasthttp

import (
	"fmt"
	"testing"
	"time"
)

func TestClient_GetWithRetry(t *testing.T) {
	client := New(
		WithReadTimeout(DefaultReadTimeout),
		WithWriteTimeout(DefaultWriteTimeout),
		WithMaxConnWaitTimeout(DefaultMaxConnWaitTimeout),
		WithMaxIdleConnDuration(DefaultMaxIdleConnDuration),
		WithMaxConnsPerHost(DefaultMaxConnsPerHost),
		WithConcurrency(DefaultConcurrency),
		WithDNSCacheDuration(DefaultDNSCacheDuration),
	)
	resp, err := client.Get("https://www.baidu.com", nil, nil)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if resp == nil {
		t.Errorf("Get failed: resp is nil")
	}
	fmt.Println("Response length:", len(resp))
}

// TestNewWithOptions 测试自定义配置
func TestNewWithOptions(t *testing.T) {
	client := New(
		WithReadTimeout(10*time.Second),
		WithWriteTimeout(10*time.Second),
		WithMaxConnsPerHost(1024),
	)
	resp, err := client.Get("https://www.baidu.com", nil, nil)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if resp == nil {
		t.Errorf("Get failed: resp is nil")
	}
	fmt.Println("Response length:", len(resp))
}
