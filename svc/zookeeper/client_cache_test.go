package zookeeper

import (
	"testing"
)

// getTestZKServer 获取测试用的 Zookeeper 服务器地址
// 优先使用环境变量 ZK_TEST_SERVER，如果没有则跳过测试
func getTestZKServer(t *testing.T) string {
	server := "10.10.10.10:2181"
	return server
}

func TestClient_GetC(t *testing.T) {
	server := getTestZKServer(t)

	client, err := NewZookeeperClient(server)
	if err != nil {
		t.Fatalf("创建 Zookeeper 客户端失败: %v", err)
	}
	defer client.Close()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{
			name:    "正常获取",
			key:     "/test",
			wantErr: false,
		},
		{
			name:    "空 key",
			key:     "",
			wantErr: true,
		},
		{
			name:    "不存在的 key",
			key:     "/test/nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := client.GetC(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetC() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && value == "" {
				t.Errorf("GetC() 返回空值")
			}
		})
	}
}

func TestClient_GetChildC(t *testing.T) {
	server := getTestZKServer(t)

	client, err := NewZookeeperClient(server)
	if err != nil {
		t.Fatalf("创建 Zookeeper 客户端失败: %v", err)
	}
	defer client.Close()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{
			name:    "正常获取子节点",
			key:     "/server_list/wechat",
			wantErr: false,
		},
		{
			name:    "空 key",
			key:     "",
			wantErr: true,
		},
		{
			name:    "不存在的 key",
			key:     "/test/nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := client.GetChildC(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetChildC() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == nil {
				t.Errorf("GetChildC() 返回 nil")
			}
		})
	}
}

func TestClient_GetChildKeyC(t *testing.T) {
	server := getTestZKServer(t)

	client, err := NewZookeeperClient(server)
	if err != nil {
		t.Fatalf("创建 Zookeeper 客户端失败: %v", err)
	}
	defer client.Close()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{
			name:    "正常获取子节点 key 列表",
			key:     "/server_list",
			wantErr: false,
		},
		{
			name:    "空 key",
			key:     "",
			wantErr: true,
		},
		{
			name:    "不存在的 key",
			key:     "/test/nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := client.GetChildKeyC(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetChildKeyC() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == nil {
				t.Errorf("GetChildKeyC() 返回 nil")
			}
		})
	}
}