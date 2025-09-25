package fasthttp

import (
	"fmt"
	"testing"
)

func TestClient_GetWithRetry(t *testing.T) {
	client := NewClient(ClientConf{})
	resp, err := client.Get("https://www.baidu.com", nil, nil)
	if err != nil {
		t.Errorf("GetWithRetry failed: %v", err)
	}
	if resp == nil {
		t.Errorf("GetWithRetry failed: resp is nil")
	}
	fmt.Println(string(resp))
}
