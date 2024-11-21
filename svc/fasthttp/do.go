package fasthttp

import (
	"encoding/json"
	"errors"

	"github.com/valyala/fasthttp"
)

func (c *Client) Get(url string, args map[string]string, headers map[string]string, retry int) ([]byte, error) {
	if c == nil {
		return []byte{}, errors.New("fasthttp client is nil")
	}

	// 从请求池中分别获取一个request、response实例
	req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	// 回收实例到请求池
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()

	// 设置请求方式
	req.Header.SetMethod(fasthttp.MethodGet)

	// 设置请求地址
	req.SetRequestURI(url)

	// 设置参数
	var arg fasthttp.Args
	for key, val := range args {
		arg.Add(key, val)
	}
	req.URI().SetQueryString(arg.String())

	// 设置请求ContentType
	req.Header.SetContentType("application/json")
	// 设置header信息
	for headerKey, headerVal := range headers {
		req.Header.Add(headerKey, headerVal)
	}

	var b []byte
	var err error
	// 发起请求
	for i := 0; i < retry; i++ {
		if err = c.Do(req, resp); err == nil {
			return resp.Body(), nil
		}
	}
	return b, err
}

func (c *Client) Post(url string, args map[string]any, headers map[string]string, retry int) ([]byte, error) {
	if c == nil {
		return []byte{}, errors.New("fasthttp client is nil")
	}

	// 从请求池中分别获取一个request、response实例
	req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	// 回收实例到请求池
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()

	// 设置请求方式
	req.Header.SetMethod(fasthttp.MethodPost)

	// 设置请求地址
	req.SetRequestURI(url)

	// 设置参数
	marshal, _ := json.Marshal(args)
	req.SetBodyRaw(marshal)

	// 设置请求ContentType
	req.Header.SetContentType("application/json")
	// 设置header信息
	for headerKey, headerVal := range headers {
		req.Header.Add(headerKey, headerVal)
	}

	var b []byte
	var err error
	// 发起请求
	for i := 0; i < retry; i++ {
		if err = c.Do(req, resp); err == nil {
			return resp.Body(), nil
		}
	}
	return b, err
}
