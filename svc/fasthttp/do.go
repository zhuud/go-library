package fasthttp

import (
	"encoding/json"
	"fmt"

	"github.com/avast/retry-go/v4"
	"github.com/valyala/fasthttp"
)

// Get 执行GET请求
func (c *Client) Get(url string, args map[string]string, headers map[string]string) ([]byte, error) {
	return c.doRequestWithRetry(fasthttp.MethodGet, url, args, nil, headers, &RetryConf{
		MaxAttempts: 1,
	})
}

func (c *Client) GetWithRetry(url string, args map[string]string, headers map[string]string, retryConfig *RetryConf) ([]byte, error) {
	return c.doRequestWithRetry(fasthttp.MethodGet, url, args, nil, headers, retryConfig)
}

// Post 执行POST请求
func (c *Client) Post(url string, args map[string]any, headers map[string]string) ([]byte, error) {
	return c.doRequestWithRetry(fasthttp.MethodPost, url, nil, args, headers, &RetryConf{
		MaxAttempts: 1,
	})
}

func (c *Client) PostWithRetry(url string, args map[string]any, headers map[string]string, retryConfig *RetryConf) ([]byte, error) {
	return c.doRequestWithRetry(fasthttp.MethodPost, url, nil, args, headers, retryConfig)
}

// doRequest 统一的请求处理方法
func (c *Client) doRequestWithRetry(method, url string, queryArgs map[string]string, bodyArgs map[string]any, headers map[string]string, retryConfig *RetryConf) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("fasthttp.doRequestWithRetry fasthttp client is nil")
	}

	var result []byte

	// 使用retry-go实现优雅重试
	err := retry.Do(
		func() error {
			// 从请求池中分别获取一个request、response实例
			req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
			// 回收实例到请求池
			defer func() {
				fasthttp.ReleaseRequest(req)
				fasthttp.ReleaseResponse(resp)
			}()

			// 设置请求方式
			req.Header.SetMethod(method)

			// 设置请求地址
			req.SetRequestURI(url)

			// 设置查询参数
			if len(queryArgs) > 0 {
				var arg fasthttp.Args
				for key, val := range queryArgs {
					arg.Add(key, val)
				}
				req.URI().SetQueryString(arg.String())
			}

			// 设置请求体（支持GET和POST请求）
			if len(bodyArgs) > 0 {
				marshal, err := json.Marshal(bodyArgs)
				if err != nil {
					return fmt.Errorf("fasthttp.doRequestWithRetry marshal request body failed: %w", err)
				}
				req.SetBodyRaw(marshal)
			}

			// 设置请求ContentType
			req.Header.SetContentType("application/json")

			// 设置header信息
			for headerKey, headerVal := range headers {
				req.Header.Add(headerKey, headerVal)
			}

			// 发起请求
			if err := c.Do(req, resp); err != nil {
				return err
			}

			// 保存响应结果
			result = make([]byte, len(resp.Body()))
			copy(result, resp.Body())
			return nil
		},
		retry.Attempts(retryConfig.MaxAttempts),
		retry.Delay(retryConfig.InitialDelay),
		retry.DelayType(retryConfig.DelayType),
		retry.MaxDelay(retryConfig.MaxDelay),
	)

	if err != nil {
		return nil, fmt.Errorf("fasthttp.doRequestWithRetry request failed after retries: %d, error: %w", retryConfig.MaxAttempts, err)
	}

	return result, nil
}
