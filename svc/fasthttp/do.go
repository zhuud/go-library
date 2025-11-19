package fasthttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/avast/retry-go/v4"
	"github.com/valyala/fasthttp"
)

// Get 执行GET请求（支持请求配置）
func (c *Client) Get(requestURL string, args map[string]string, headers map[string]string, opts ...RequestOptionFunc) ([]byte, error) {
	// 将查询参数拼接到 URL（使用 net/url 包安全处理）
	if len(args) > 0 {
		parsedURL, err := url.Parse(requestURL)
		if err != nil {
			return nil, fmt.Errorf("fasthttp.Get parse URL failed: %w", err)
		}

		// 合并查询参数
		query := parsedURL.Query()
		for key, val := range args {
			query.Add(key, val)
		}
		parsedURL.RawQuery = query.Encode()
		requestURL = parsedURL.String()
	}
	return c.doRequest(fasthttp.MethodGet, requestURL, nil, headers, opts...)
}

// Post 执行POST请求（支持请求配置）
// args 会被序列化为 JSON 格式
func (c *Client) Post(url string, args map[string]any, headers map[string]string, opts ...RequestOptionFunc) ([]byte, error) {
	// 将 args 序列化为 JSON
	var body []byte
	var err error
	if len(args) > 0 {
		body, err = json.Marshal(args)
		if err != nil {
			return nil, fmt.Errorf("fasthttp.Post marshal request body failed: %w", err)
		}
	}
	// 强制设置 Content-Type 为 application/json
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["Content-Type"] = "application/json"
	return c.doRequest(fasthttp.MethodPost, url, body, headers, opts...)
}

// PostRaw 执行POST请求，直接使用 []byte 作为请求体
// body 是已序列化的请求体内容，不会进行任何序列化处理
func (c *Client) PostRaw(url string, body []byte, headers map[string]string, opts ...RequestOptionFunc) ([]byte, error) {
	return c.doRequest(fasthttp.MethodPost, url, body, headers, opts...)
}

// doRequest 统一的请求处理方法
// body 可以是 nil（无请求体）或 []byte（已序列化的请求体）
func (c *Client) doRequest(method, url string, body []byte, headers map[string]string, opts ...RequestOptionFunc) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("fasthttp.doRequest fasthttp client is nil")
	}

	// 默认请求配置
	requestConfig := &RequestConf{
		Context: context.Background(),
		Retry: &retryConf{
			MaxAttempts: 1,
		},
	}

	// 应用请求配置选项
	for _, opt := range opts {
		opt(requestConfig)
	}

	var result []byte

	// 使用retry-go实现优雅重试
	err := retry.Do(
		func() error {
			// 如果配置了断路器，使用断路器包装请求执行
			if c.brk != nil {
				return c.brk.DoWithAcceptableCtx(requestConfig.Context, func() error {
					var err error
					result, err = c.doHTTPRequest(method, url, body, headers)
					return err
				}, acceptable)
			}
			// 如果没有配置断路器，直接执行请求
			var err error
			result, err = c.doHTTPRequest(method, url, body, headers)
			return err
		},
		retry.Attempts(requestConfig.Retry.MaxAttempts),
		retry.Delay(requestConfig.Retry.InitialDelay),
		retry.DelayType(requestConfig.Retry.DelayType),
		retry.MaxDelay(requestConfig.Retry.MaxDelay),
		retry.Context(requestConfig.Context),
	)

	if err != nil {
		return nil, fmt.Errorf("fasthttp.doRequest request failed after retries: %d, error: %w", requestConfig.Retry.MaxAttempts, err)
	}

	return result, nil
}

// doHTTPRequest 执行 HTTP 请求（底层实现）
// body 可以是 nil（无请求体）或 []byte（已序列化的请求体）
func (c *Client) doHTTPRequest(method, url string, body []byte, headers map[string]string) ([]byte, error) {
	// 从请求池中分别获取一个request、response实例
	req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	// 回收实例到请求池
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()

	// 设置请求方式
	req.Header.SetMethod(method)

	// 设置请求地址（URL 中已包含查询参数）
	req.SetRequestURI(url)

	// 设置用户自定义的header信息
	for headerKey, headerVal := range headers {
		req.Header.Add(headerKey, headerVal)
	}

	// 设置请求体（直接使用传入的 []byte，不进行任何序列化）
	if len(body) > 0 {
		req.SetBodyRaw(body)
	}

	// 发起请求
	if err := c.Do(req, resp); err != nil {
		return nil, err
	}

	// 保存响应结果
	result := make([]byte, len(resp.Body()))
	copy(result, resp.Body())
	return result, nil
}

// acceptable 判断错误是否可接受（用于断路器）
// 网络错误、超时错误等不可接受，需要触发熔断
func acceptable(err error) bool {
	if err == nil {
		return true
	}

	// 网络错误不可接受
	var netErr net.Error
	if errors.As(err, &netErr) {
		return false
	}

	// 其他错误视为可接受（如业务错误、HTTP错误等）
	return true
}