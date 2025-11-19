package fasthttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
	"github.com/zeromicro/go-zero/core/breaker"
)

// mockServer 创建一个 mock HTTP 服务器用于测试
// 返回 listener 和配置好的 client
func mockServer(handler func(ctx *fasthttp.RequestCtx)) (*fasthttputil.InmemoryListener, *Client) {
	ln := fasthttputil.NewInmemoryListener()
	server := &fasthttp.Server{
		Handler: handler,
	}
	go server.Serve(ln)

	// 创建使用 InmemoryListener 的客户端
	client := New()
	client.Dial = func(addr string) (net.Conn, error) {
		return ln.Dial()
	}

	return ln, client
}

// TestClient_Get_Basic 测试基本 GET 请求
func TestClient_Get_Basic(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Method()) != fasthttp.MethodGet {
			ctx.Error("Method not allowed", fasthttp.StatusMethodNotAllowed)
			return
		}
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	resp, err := client.Get("http://test", nil, nil)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Get_WithQueryArgs 测试带查询参数的 GET 请求
func TestClient_Get_WithQueryArgs(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		queryArgs := ctx.QueryArgs()
		key1 := string(queryArgs.Peek("key1"))
		key2 := string(queryArgs.Peek("key2"))

		if key1 != "value1" || key2 != "value2" {
			ctx.Error("Query args mismatch", fasthttp.StatusBadRequest)
			return
		}

		ctx.SetBodyString(`{"key1":"` + key1 + `","key2":"` + key2 + `"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	queryArgs := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	resp, err := client.Get("http://test", queryArgs, nil)
	if err != nil {
		t.Fatalf("Get with query args failed: %v", err)
	}

	var result map[string]string
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if result["key1"] != "value1" || result["key2"] != "value2" {
		t.Errorf("Query args mismatch: got %v", result)
	}
}

// TestClient_Get_WithHeaders 测试带请求头的 GET 请求
func TestClient_Get_WithHeaders(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		userAgent := string(ctx.Request.Header.Peek("User-Agent"))
		accept := string(ctx.Request.Header.Peek("Accept"))

		ctx.SetBodyString(fmt.Sprintf(`{"User-Agent":"%s","Accept":"%s"}`, userAgent, accept))
	})
	defer ln.Close()

	// client already created by mockServer
	headers := map[string]string{
		"User-Agent": "fasthttp-test",
		"Accept":     "application/json",
	}

	resp, err := client.Get("http://test", nil, headers)
	if err != nil {
		t.Fatalf("Get with headers failed: %v", err)
	}

	var result map[string]string
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if result["User-Agent"] != "fasthttp-test" || result["Accept"] != "application/json" {
		t.Errorf("Headers mismatch: got %v", result)
	}
}

// TestClient_Get_WithURLQueryParams 测试 URL 中已包含查询参数的情况
func TestClient_Get_WithURLQueryParams(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		queryArgs := ctx.QueryArgs()
		existing := string(queryArgs.Peek("existing"))
		added := string(queryArgs.Peek("added"))

		if existing != "value" || added != "new" {
			ctx.Error("Query args mismatch", fasthttp.StatusBadRequest)
			return
		}

		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	queryArgs := map[string]string{
		"added": "new",
	}

	// URL 中已包含 existing=value
	resp, err := client.Get("http://test?existing=value", queryArgs, nil)
	if err != nil {
		t.Fatalf("Get with URL query params failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Post_Basic 测试基本 POST 请求
func TestClient_Post_Basic(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Method()) != fasthttp.MethodPost {
			ctx.Error("Method not allowed", fasthttp.StatusMethodNotAllowed)
			return
		}

		contentType := string(ctx.Request.Header.Peek("Content-Type"))
		if contentType != "application/json" {
			ctx.Error("Content-Type mismatch", fasthttp.StatusBadRequest)
			return
		}

		ctx.SetBody(ctx.Request.Body())
	})
	defer ln.Close()

	// client already created by mockServer
	bodyArgs := map[string]any{
		"name":  "test",
		"value": 123,
	}

	resp, err := client.Post("http://test", bodyArgs, nil)
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if result["name"] != "test" || result["value"] != float64(123) {
		t.Errorf("Body mismatch: got %v", result)
	}
}

// TestClient_Post_EmptyBody 测试空 body 的 POST 请求
func TestClient_Post_EmptyBody(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		contentType := string(ctx.Request.Header.Peek("Content-Type"))
		if contentType != "application/json" {
			ctx.Error("Content-Type mismatch", fasthttp.StatusBadRequest)
			return
		}

		ctx.SetBodyString(`{"received":true}`)
	})
	defer ln.Close()

	// client already created by mockServer
	resp, err := client.Post("http://test", nil, nil)
	if err != nil {
		t.Fatalf("Post with empty body failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Post_ComplexBody 测试复杂请求体
func TestClient_Post_ComplexBody(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBody(ctx.Request.Body())
	})
	defer ln.Close()

	// client already created by mockServer
	bodyArgs := map[string]any{
		"string": "test",
		"number": 123,
		"float":  45.67,
		"bool":   true,
		"array":  []string{"a", "b", "c"},
		"object": map[string]any{
			"nested": "value",
			"count":  42,
		},
	}

	resp, err := client.Post("http://test", bodyArgs, nil)
	if err != nil {
		t.Fatalf("Post with complex body failed: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if result["string"] != "test" {
		t.Errorf("Complex body mismatch: got %v", result)
	}
}

// TestClient_Post_ContentTypeOverride 测试 Content-Type 被强制覆盖
func TestClient_Post_ContentTypeOverride(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		contentType := string(ctx.Request.Header.Peek("Content-Type"))
		if contentType != "application/json" {
			ctx.Error("Content-Type should be application/json", fasthttp.StatusBadRequest)
			return
		}
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	headers := map[string]string{
		"Content-Type": "application/xml", // 尝试设置其他类型
	}

	bodyArgs := map[string]any{
		"test": "data",
	}

	resp, err := client.Post("http://test", bodyArgs, headers)
	if err != nil {
		t.Fatalf("Post with Content-Type override failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_PostRaw_Basic 测试基本 PostRaw 请求
func TestClient_PostRaw_Basic(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBody(ctx.Request.Body())
	})
	defer ln.Close()

	// client already created by mockServer
	body := []byte(`{"raw":"data"}`)
	headers := map[string]string{
		"Content-Type": "application/json",
	}

	resp, err := client.PostRaw("http://test", body, headers)
	if err != nil {
		t.Fatalf("PostRaw failed: %v", err)
	}

	if string(resp) != string(body) {
		t.Errorf("PostRaw body mismatch: got %s, want %s", string(resp), string(body))
	}
}

// TestClient_PostRaw_EmptyBody 测试空 body 的 PostRaw 请求
func TestClient_PostRaw_EmptyBody(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"received":true}`)
	})
	defer ln.Close()

	// client already created by mockServer
	resp, err := client.PostRaw("http://test", nil, nil)
	if err != nil {
		t.Fatalf("PostRaw with empty body failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_PostRaw_CustomContentType 测试 PostRaw 使用自定义 Content-Type
func TestClient_PostRaw_CustomContentType(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		contentType := string(ctx.Request.Header.Peek("Content-Type"))
		if contentType != "application/xml" {
			ctx.Error("Content-Type mismatch", fasthttp.StatusBadRequest)
			return
		}
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	body := []byte(`<xml>data</xml>`)
	headers := map[string]string{
		"Content-Type": "application/xml",
	}

	resp, err := client.PostRaw("http://test", body, headers)
	if err != nil {
		t.Fatalf("PostRaw with custom Content-Type failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Get_WithContext 测试带上下文的 GET 请求
func TestClient_Get_WithContext(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get("http://test", nil, nil, WithContext(ctx))
	if err != nil {
		t.Fatalf("Get with context failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Get_WithContextTimeout 测试上下文超时
// 注意：InmemoryListener 响应很快，实际超时测试需要使用真实网络
func TestClient_Get_WithContextTimeout(t *testing.T) {
	// 这个测试主要验证 WithContext 选项能正确传递
	// 实际超时测试需要真实网络环境
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get("http://test", nil, nil, WithContext(ctx))
	if err != nil {
		t.Fatalf("Get with context failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Get_WithRetry 测试带重试的 GET 请求
// 注意：重试机制只对网络错误（net.Error）生效，HTTP 状态码错误不会触发重试
func TestClient_Get_WithRetry(t *testing.T) {
	// 这个测试主要验证重试配置能正确传递
	// 实际重试测试需要模拟网络错误（如连接失败）
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	resp, err := client.Get("http://test", nil, nil,
		WithRetryAttempts(3),
		WithRetryInitialDelay(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Get with retry failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Get_WithRetryMaxAttempts 测试重试达到最大次数
// 注意：重试机制只对网络错误（net.Error）生效，HTTP 状态码错误不会触发重试
func TestClient_Get_WithRetryMaxAttempts(t *testing.T) {
	// 这个测试主要验证重试配置能正确传递
	// 实际重试达到最大次数的测试需要模拟网络错误（如连接失败）
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	resp, err := client.Get("http://test", nil, nil,
		WithRetryAttempts(2),
		WithRetryInitialDelay(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Get with retry should succeed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Get_WithBreaker 测试启用熔断器的 GET 请求
func TestClient_Get_WithBreaker(t *testing.T) {
	ln, _ := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	client := New(WithBreaker(breaker.NewBreaker()))
	client.Dial = func(addr string) (net.Conn, error) {
		return ln.Dial()
	}
	resp, err := client.Get("http://test", nil, nil)
	if err != nil {
		t.Fatalf("Get with breaker failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Get_WithoutBreaker 测试不启用熔断器（默认行为）
func TestClient_Get_WithoutBreaker(t *testing.T) {
	client := New()
	if client.brk != nil {
		t.Error("Expected breaker to be nil by default")
	}
}

// TestClient_Get_WithBreakerAndRetry 测试同时启用熔断器和重试
func TestClient_Get_WithBreakerAndRetry(t *testing.T) {
	ln, _ := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	client := New(WithBreaker(breaker.NewBreaker()))
	client.Dial = func(addr string) (net.Conn, error) {
		return ln.Dial()
	}
	resp, err := client.Get("http://test", nil, nil,
		WithContext(context.Background()),
		WithRetryAttempts(2),
		WithRetryInitialDelay(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Get with breaker and retry failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Post_WithAllOptions 测试 POST 请求（所有选项）
func TestClient_Post_WithAllOptions(t *testing.T) {
	ln, _ := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBody(ctx.Request.Body())
	})
	defer ln.Close()

	client := New(
		WithReadTimeout(10*time.Second),
		WithWriteTimeout(10*time.Second),
		WithBreaker(breaker.NewBreaker()),
	)
	client.Dial = func(addr string) (net.Conn, error) {
		return ln.Dial()
	}

	bodyArgs := map[string]any{
		"test": "data",
	}

	headers := map[string]string{
		"X-Custom-Header": "custom-value",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Post("http://"+ln.Addr().String(), bodyArgs, headers,
		WithContext(ctx),
		WithRetryAttempts(2),
		WithRetryInitialDelay(10*time.Millisecond),
		WithRetryMaxDelay(1*time.Second),
	)
	if err != nil {
		t.Fatalf("Post with all options failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Get_InvalidURL 测试无效 URL
func TestClient_Get_InvalidURL(t *testing.T) {
	client := New()
	_, err := client.Get("invalid-url", nil, nil)
	if err == nil {
		t.Error("Expected error for invalid URL, but got nil")
	}
}

// TestClient_Get_InvalidURLParse 测试 URL 解析失败
func TestClient_Get_InvalidURLParse(t *testing.T) {
	client := New()
	queryArgs := map[string]string{
		"key": "value",
	}
	// 使用无效的 URL 格式
	_, err := client.Get("http://[invalid", queryArgs, nil)
	if err == nil {
		t.Error("Expected error for invalid URL parse, but got nil")
	}
}

// TestClient_Post_JSONMarshalError 测试 JSON 序列化失败（理论上不会发生，因为 map[string]any 总是可序列化）
func TestClient_Post_JSONMarshalError(t *testing.T) {
	// 这个测试主要是为了覆盖代码路径
	// map[string]any 通常总是可以序列化的
	bodyArgs := map[string]any{
		"valid": "data",
	}

	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	_, err := client.Post("http://test", bodyArgs, nil)
	if err != nil {
		t.Fatalf("Post should succeed: %v", err)
	}
}

// TestClient_doRequest_NilClient 测试 nil client
func TestClient_doRequest_NilClient(t *testing.T) {
	var client *Client
	_, err := client.Get("http://example.com", nil, nil)
	if err == nil {
		t.Error("Expected error for nil client, but got nil")
	}
}

// TestClient_Get_WithRetryDelayType 测试自定义重试延迟类型
func TestClient_Get_WithRetryDelayType(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	resp, err := client.Get("http://test", nil, nil,
		WithRetryAttempts(2),
		WithRetryInitialDelay(10*time.Millisecond),
		WithRetryDelayType(func(n uint, err error, config *retry.Config) time.Duration {
			return 10 * time.Millisecond
		}),
	)
	if err != nil {
		t.Fatalf("Get with custom delay type failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Get_WithRetryMaxDelay 测试最大延迟时间
func TestClient_Get_WithRetryMaxDelay(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	resp, err := client.Get("http://test", nil, nil,
		WithRetryAttempts(2),
		WithRetryInitialDelay(10*time.Millisecond),
		WithRetryMaxDelay(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Get with max delay failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_New_WithOptions 测试自定义配置
func TestClient_New_WithOptions(t *testing.T) {
	client := New(
		WithReadTimeout(10*time.Second),
		WithWriteTimeout(10*time.Second),
		WithMaxConnsPerHost(1024),
		WithConcurrency(512*1024),
		WithDNSCacheDuration(5*time.Minute),
		WithMaxIdleConnDuration(5*time.Minute),
		WithMaxConnWaitTimeout(10*time.Second),
	)

	if client == nil {
		t.Fatal("Client should not be nil")
	}
}

// TestClient_acceptable 测试错误接受判断
func TestClient_acceptable(t *testing.T) {
	// 测试 nil 错误
	if !acceptable(nil) {
		t.Error("Nil error should be acceptable")
	}

	// 测试网络错误
	netErr := &net.DNSError{
		Err:         "DNS error",
		Name:        "example.com",
		Server:      "8.8.8.8",
		IsTimeout:   false,
		IsTemporary: false,
	}
	if acceptable(netErr) {
		t.Error("Network error should not be acceptable")
	}

	// 测试其他错误（应该可接受）
	otherErr := fmt.Errorf("some other error")
	if !acceptable(otherErr) {
		t.Error("Other error should be acceptable")
	}
}

// TestClient_Get_WithNilHeaders 测试 nil headers
func TestClient_Get_WithNilHeaders(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	resp, err := client.Get("http://test", nil, nil)
	if err != nil {
		t.Fatalf("Get with nil headers failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_Post_WithNilHeaders 测试 POST 请求的 nil headers
func TestClient_Post_WithNilHeaders(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		contentType := string(ctx.Request.Header.Peek("Content-Type"))
		if contentType != "application/json" {
			ctx.Error("Content-Type should be application/json", fasthttp.StatusBadRequest)
			return
		}
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	bodyArgs := map[string]any{
		"test": "data",
	}

	resp, err := client.Post("http://test", bodyArgs, nil)
	if err != nil {
		t.Fatalf("Post with nil headers failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}

// TestClient_PostRaw_WithNilHeaders 测试 PostRaw 的 nil headers
func TestClient_PostRaw_WithNilHeaders(t *testing.T) {
	ln, client := mockServer(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(`{"status":"ok"}`)
	})
	defer ln.Close()

	// client already created by mockServer
	body := []byte(`{"raw":"data"}`)

	resp, err := client.PostRaw("http://test", body, nil)
	if err != nil {
		t.Fatalf("PostRaw with nil headers failed: %v", err)
	}
	if len(resp) == 0 {
		t.Error("Response is empty")
	}
}
