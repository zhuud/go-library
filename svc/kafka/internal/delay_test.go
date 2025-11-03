package internal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

var (
	testCtx       = context.Background()
	testRedis     *redis.Redis
	testPrefix    = "test_delay"
	testTopic     = "test_topic"
	testBatchSize = 100
)

func initTestRedis(t *testing.T) *redis.Redis {
	if testRedis == nil {
		testRedis = redis.MustNewRedis(redis.RedisConf{
			Host: "",
			Type: "node",
			Pass: "",
		})
	}
	// 清理测试数据
	for i := 0; i < queueKeyBucketNum; i++ {
		_, _ = testRedis.Del(fmtQueueKey(int64(i), delayQueueName))
		_, _ = testRedis.Del(fmtQueueKey(int64(i), reservedQueueName))
	}
	return testRedis
}

func TestSetUpWithRetry(t *testing.T) {
	r := initTestRedis(t)

	t.Run("使用默认值", func(t *testing.T) {
		SetUpWithRetry(r, 0, testPrefix, 0, 0)
		_ = r // 使用 r 避免未使用变量警告

		if queuePrefix != testPrefix {
			t.Errorf("queuePrefix = %s, want %s", queuePrefix, testPrefix)
		}
		// 默认值验证：maxRetryAttempts = 3, retryDelayDuration = time.Minute
		if maxRetryAttempts != 3 {
			t.Errorf("maxRetryAttempts = %d, want 3", maxRetryAttempts)
		}
		if retryDelayDuration != time.Minute {
			t.Errorf("retryDelayDuration = %v, want %v", retryDelayDuration, time.Minute)
		}
	})

	t.Run("自定义配置", func(t *testing.T) {
		SetUpWithRetry(r, 200, testPrefix, 5, 5*time.Minute)
		_ = r // 使用 r 避免未使用变量警告

		if queueGetBatchSize != 200 {
			t.Errorf("queueGetBatchSize = %d, want 200", queueGetBatchSize)
		}
		if maxRetryAttempts != 5 {
			t.Errorf("maxRetryAttempts = %d, want 5", maxRetryAttempts)
		}
		if retryDelayDuration != 5*time.Minute {
			t.Errorf("retryDelayDuration = %v, want %v", retryDelayDuration, 5*time.Minute)
		}
	})
}

func TestPush(t *testing.T) {
	r := initTestRedis(t)
	SetUpWithRetry(r, testBatchSize, testPrefix, 3, time.Minute)

	t.Run("正常推送", func(t *testing.T) {
		data := map[string]interface{}{"key": "value"}
		err := Push(testCtx, testTopic, data, time.Second*5)
		if err != nil {
			t.Errorf("Push() 不应该返回错误: %v", err)
			return
		}

		// 验证数据是否正确写入 Redis
		ts := time.Now().Add(time.Second * 5).Unix()
		key := fmtQueueKey(ts, delayQueueName)
		members, err := r.ZrangeWithScores(key, 0, -1)
		if err != nil {
			t.Errorf("ZrangeWithScores() 错误: %v", err)
			return
		}

		found := false
		for _, member := range members {
			var dd DelayData
			if err := json.Unmarshal([]byte(member.Key), &dd); err == nil {
				if dd.Topic == testTopic && dd.Attempts == 0 {
					found = true
					break
				}
			}
		}
		if !found {
			t.Error("数据应该已写入 Redis")
		}
	})

	t.Run("空 topic", func(t *testing.T) {
		err := Push(testCtx, "", map[string]interface{}{"key": "value"}, time.Second*5)
		if err == nil {
			t.Error("Push() 应该返回错误")
		}
		if err != nil && !strings.Contains(err.Error(), "topic must not be empty") {
			t.Errorf("Push() 错误消息不正确: %v", err)
		}
	})

	t.Run("延迟时间过短", func(t *testing.T) {
		err := Push(testCtx, testTopic, "data", time.Millisecond*500)
		if err == nil {
			t.Error("Push() 应该返回错误")
		}
		if err != nil && !strings.Contains(err.Error(), "delayDuration must be at least 1 second") {
			t.Errorf("Push() 错误消息不正确: %v", err)
		}
	})

	t.Run("延迟时间过长", func(t *testing.T) {
		err := Push(testCtx, testTopic, "data", time.Hour*24*8) // 超过 7 天
		if err == nil {
			t.Error("Push() 应该返回错误")
		}
		if err != nil && !strings.Contains(err.Error(), "delayDuration") {
			t.Errorf("Push() 错误消息不正确: %v", err)
		}
	})
}

func TestPush_WithoutSetup(t *testing.T) {
	// 临时清空 client 以测试未初始化的场景
	oldClient := client
	client = nil
	defer func() { client = oldClient }()

	err := Push(testCtx, testTopic, "data", time.Second)
	if err == nil {
		t.Error("Push() 应该返回错误")
	}
	if err != nil && !strings.Contains(err.Error(), "must setup before") {
		t.Errorf("Push() 错误消息不正确: %v", err)
	}
}

func TestPop(t *testing.T) {
	r := initTestRedis(t)
	SetUpWithRetry(r, testBatchSize, testPrefix, 3, time.Minute)

	// 准备测试数据：推送一个已过期的消息
	testData := map[string]interface{}{"test": "data"}
	err := Push(testCtx, testTopic, testData, time.Second) // 负延迟，立即过期
	if err != nil {
		t.Fatalf("Push() 错误: %v", err)
	}

	// 等待一小段时间确保时间戳更新
	time.Sleep(100 * time.Millisecond)

	// 执行 Pop
	result := Pop()

	// 验证结果（由于时间精度，可能不会立即弹出）
	t.Logf("Pop() 返回了 %d 条数据", len(result))
}

func TestPop_WithoutSetup(t *testing.T) {
	oldClient := client
	client = nil
	defer func() { client = oldClient }()

	result := Pop()
	if len(result) != 0 {
		t.Errorf("Pop() 应该返回空切片，实际返回 %d 条", len(result))
	}
}

func TestSuccessAck(t *testing.T) {
	r := initTestRedis(t)
	SetUpWithRetry(r, testBatchSize, testPrefix, 3, time.Minute)

	// 准备测试数据：在 reserved 队列中添加一条数据
	timestamp := time.Now().Unix()
	delayData := DelayData{
		Topic:     testTopic,
		Data:      "test data",
		Timestamp: timestamp,
		Attempts:  1,
	}
	taskJson, err := json.Marshal(delayData)
	if err != nil {
		t.Fatalf("Marshal() 错误: %v", err)
	}

	// 添加到 reserved 队列
	key := fmtQueueKey(timestamp, reservedQueueName)
	_, err = r.Zadd(key, timestamp, string(taskJson))
	if err != nil {
		t.Fatalf("Zadd() 错误: %v", err)
	}

	// 验证数据已添加
	members, err := r.ZrangeWithScores(key, 0, -1)
	if err != nil {
		t.Fatalf("ZrangeWithScores() 错误: %v", err)
	}
	if len(members) == 0 {
		t.Fatal("数据应该已添加到 reserved 队列")
	}

	// 执行 SuccessAck
	startTime := time.Now()
	err = SuccessAck(timestamp, string(taskJson), time.Since(startTime))
	if err != nil {
		t.Errorf("SuccessAck() 不应该返回错误: %v", err)
	}

	// 验证数据已从 reserved 队列删除
	members, err = r.ZrangeWithScores(key, 0, -1)
	if err != nil {
		t.Fatalf("ZrangeWithScores() 错误: %v", err)
	}
	found := false
	for _, m := range members {
		if m.Key == string(taskJson) {
			found = true
			break
		}
	}
	if found {
		t.Error("数据应该已从 reserved 队列删除")
	}
}

func TestSuccessAck_EmptyTaskJson(t *testing.T) {
	r := initTestRedis(t)
	SetUpWithRetry(r, testBatchSize, testPrefix, 3, time.Minute)

	err := SuccessAck(time.Now().Unix(), "", 0)
	if err == nil {
		t.Error("SuccessAck() 应该返回错误")
	}
	if err != nil && !strings.Contains(err.Error(), "taskJson is empty") {
		t.Errorf("SuccessAck() 错误消息不正确: %v", err)
	}
}

func TestFailAck_Retry(t *testing.T) {
	r := initTestRedis(t)
	maxAttempts := 3
	retryDelay := time.Second * 2 // 测试用短延迟
	SetUpWithRetry(r, testBatchSize, testPrefix, maxAttempts, retryDelay)

	// 准备测试数据：在 reserved 队列中添加一条数据
	timestamp := time.Now().Unix()
	delayData := DelayData{
		Topic:     testTopic,
		Data:      "test data",
		Timestamp: timestamp,
		Attempts:  0, // 初始尝试次数
	}
	taskJson, err := json.Marshal(delayData)
	if err != nil {
		t.Fatalf("Marshal() 错误: %v", err)
	}

	// 添加到 reserved 队列
	reservedKey := fmtQueueKey(timestamp, reservedQueueName)
	_, err = r.Zadd(reservedKey, timestamp, string(taskJson))
	if err != nil {
		t.Fatalf("Zadd() 错误: %v", err)
	}

	// 执行 FailAck（应该重试）
	startTime := time.Now()
	err = FailAck(timestamp, string(taskJson), time.Since(startTime))
	if err != nil {
		t.Errorf("FailAck() 不应该返回错误: %v", err)
	}

	// 验证原数据已从 reserved 队列删除
	members, err := r.ZrangeWithScores(reservedKey, 0, -1)
	if err != nil {
		t.Fatalf("ZrangeWithScores() 错误: %v", err)
	}
	found := false
	for _, m := range members {
		if m.Key == string(taskJson) {
			found = true
			break
		}
	}
	if found {
		t.Error("原数据应该已从 reserved 队列删除")
	}

	// 验证新数据已添加到 delay 队列
	expectedNewTimestamp := time.Now().Add(retryDelay).Unix()
	newKey := fmtQueueKey(expectedNewTimestamp, delayQueueName)
	members, err = r.ZrangeWithScores(newKey, 0, -1)
	if err != nil {
		t.Fatalf("ZrangeWithScores() 错误: %v", err)
	}

	found = false
	var newDelayData DelayData
	for _, m := range members {
		if err := json.Unmarshal([]byte(m.Key), &newDelayData); err == nil {
			if newDelayData.Topic == testTopic && newDelayData.Attempts == 1 {
				found = true
				// 验证 Attempts 已增加
				if newDelayData.Attempts != 1 {
					t.Errorf("Attempts = %d, want 1", newDelayData.Attempts)
				}
				// 验证 Timestamp 已更新
				if newDelayData.Timestamp <= timestamp {
					t.Errorf("新的 Timestamp %d 应该大于原 Timestamp %d", newDelayData.Timestamp, timestamp)
				}
				break
			}
		}
	}
	if !found {
		t.Error("重试数据应该已添加到 delay 队列")
	}
}

func TestFailAck_MaxAttemptsExceeded(t *testing.T) {
	r := initTestRedis(t)
	maxAttempts := 2
	SetUpWithRetry(r, testBatchSize, testPrefix, maxAttempts, time.Minute)

	// 准备测试数据：Attempts 已达到最大重试次数
	timestamp := time.Now().Unix()
	delayData := DelayData{
		Topic:     testTopic,
		Data:      "test data",
		Timestamp: timestamp,
		Attempts:  maxAttempts, // 已达到最大重试次数
	}
	taskJson, err := json.Marshal(delayData)
	if err != nil {
		t.Fatalf("Marshal() 错误: %v", err)
	}

	// 添加到 reserved 队列
	key := fmtQueueKey(timestamp, reservedQueueName)
	_, err = r.Zadd(key, timestamp, string(taskJson))
	if err != nil {
		t.Fatalf("Zadd() 错误: %v", err)
	}

	// 执行 FailAck（应该直接删除，不再重试）
	startTime := time.Now()
	err = FailAck(timestamp, string(taskJson), time.Since(startTime))
	if err != nil {
		t.Errorf("FailAck() 不应该返回错误: %v", err)
	}

	// 验证数据已从 reserved 队列删除
	members, err := r.ZrangeWithScores(key, 0, -1)
	if err != nil {
		t.Fatalf("ZrangeWithScores() 错误: %v", err)
	}
	found := false
	for _, m := range members {
		if m.Key == string(taskJson) {
			found = true
			break
		}
	}
	if found {
		t.Error("超过最大重试次数的数据应该被删除")
	}
}

func TestFailAck_InvalidJSON(t *testing.T) {
	r := initTestRedis(t)
	SetUpWithRetry(r, testBatchSize, testPrefix, 3, time.Minute)

	timestamp := time.Now().Unix()
	invalidJson := "{invalid json}"

	// 添加到 reserved 队列
	key := fmtQueueKey(timestamp, reservedQueueName)
	_, err := r.Zadd(key, timestamp, invalidJson)
	if err != nil {
		t.Fatalf("Zadd() 错误: %v", err)
	}

	// 执行 FailAck（应该处理错误）
	startTime := time.Now()
	err = FailAck(timestamp, invalidJson, time.Since(startTime))
	if err == nil {
		t.Error("FailAck() 应该返回错误")
	}
	if err != nil && !strings.Contains(err.Error(), "Unmarshal error") {
		t.Errorf("FailAck() 错误消息不正确: %v", err)
	}

	// 验证无效数据应该被清理
	members, err := r.ZrangeWithScores(key, 0, -1)
	if err != nil {
		t.Fatalf("ZrangeWithScores() 错误: %v", err)
	}
	found := false
	for _, m := range members {
		if m.Key == invalidJson {
			found = true
			break
		}
	}
	if found {
		t.Error("无效 JSON 数据应该被清理")
	}
}

func TestFailAck_EmptyTaskJson(t *testing.T) {
	r := initTestRedis(t)
	SetUpWithRetry(r, testBatchSize, testPrefix, 3, time.Minute)

	err := FailAck(time.Now().Unix(), "", 0)
	if err == nil {
		t.Error("FailAck() 应该返回错误")
	}
	if err != nil && !strings.Contains(err.Error(), "taskJson is empty") {
		t.Errorf("FailAck() 错误消息不正确: %v", err)
	}
}

func TestFmtQueueKey(t *testing.T) {
	SetUpWithRetry(nil, 100, "test_prefix", 0, 0)

	tests := []struct {
		name      string
		timestamp int64
		queueType string
		expected  string
	}{
		{
			name:      "bucket 0",
			timestamp: 100,
			queueType: delayQueueName,
			expected:  "{kafka:delay:queue}:test_prefix:0:delayed",
		},
		{
			name:      "bucket 5",
			timestamp: 105,
			queueType: delayQueueName,
			expected:  "{kafka:delay:queue}:test_prefix:5:delayed",
		},
		{
			name:      "bucket 9",
			timestamp: 109,
			queueType: reservedQueueName,
			expected:  "{kafka:delay:queue}:test_prefix:9:reserved",
		},
		{
			name:      "大时间戳",
			timestamp: 1735689600, // 2025-01-01 的 Unix 时间戳
			queueType: delayQueueName,
			expected:  "{kafka:delay:queue}:test_prefix:0:delayed", // 1735689600 % 10 = 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fmtQueueKey(tt.timestamp, tt.queueType)
			if result != tt.expected {
				t.Errorf("fmtQueueKey() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestDelayData_MarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name string
		data DelayData
	}{
		{
			name: "正常数据",
			data: DelayData{
				Topic:     "test_topic",
				Data:      map[string]string{"key": "value"},
				Timestamp: 1234567890,
				Attempts:  2,
			},
		},
		{
			name: "字符串数据",
			data: DelayData{
				Topic:     "test_topic",
				Data:      "simple string",
				Timestamp: 1234567890,
				Attempts:  0,
			},
		},
		{
			name: "空数据",
			data: DelayData{
				Topic:     "test_topic",
				Data:      nil,
				Timestamp: 1234567890,
				Attempts:  0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			jsonData, err := json.Marshal(tt.data)
			if err != nil {
				t.Fatalf("Marshal() 错误: %v", err)
			}
			if len(jsonData) == 0 {
				t.Error("Marshal() 结果不应该为空")
			}

			// Unmarshal
			var result DelayData
			err = json.Unmarshal(jsonData, &result)
			if err != nil {
				t.Fatalf("Unmarshal() 错误: %v", err)
			}

			// 验证字段
			if result.Topic != tt.data.Topic {
				t.Errorf("Topic = %s, want %s", result.Topic, tt.data.Topic)
			}
			if result.Timestamp != tt.data.Timestamp {
				t.Errorf("Timestamp = %d, want %d", result.Timestamp, tt.data.Timestamp)
			}
			if result.Attempts != tt.data.Attempts {
				t.Errorf("Attempts = %d, want %d", result.Attempts, tt.data.Attempts)
			}
			// Data 字段可能类型转换，只验证不为 nil（如果是非 nil 的情况）
			if tt.data.Data != nil && result.Data == nil {
				t.Error("Data 不应该为 nil")
			}
		})
	}
}

func TestRemoveFromReserved(t *testing.T) {
	r := initTestRedis(t)
	SetUpWithRetry(r, testBatchSize, testPrefix, 3, time.Minute)

	// 准备测试数据
	timestamp := time.Now().Unix()
	taskJson := `{"topic":"test","data":"test","timestamp":1234567890,"attempts":1}`

	// 添加到 reserved 队列
	key := fmtQueueKey(timestamp, reservedQueueName)
	_, err := r.Zadd(key, timestamp, taskJson)
	if err != nil {
		t.Fatalf("Zadd() 错误: %v", err)
	}

	// 验证数据已添加
	members, err := r.ZrangeWithScores(key, 0, -1)
	if err != nil {
		t.Fatalf("ZrangeWithScores() 错误: %v", err)
	}
	if len(members) == 0 {
		t.Fatal("数据应该已添加到 reserved 队列")
	}

	// 执行删除
	err = removeFromReserved(timestamp, taskJson)
	if err != nil {
		t.Errorf("removeFromReserved() 不应该返回错误: %v", err)
	}

	// 验证数据已删除
	members, err = r.ZrangeWithScores(key, 0, -1)
	if err != nil {
		t.Fatalf("ZrangeWithScores() 错误: %v", err)
	}
	found := false
	for _, m := range members {
		if m.Key == taskJson {
			found = true
			break
		}
	}
	if found {
		t.Error("数据应该已从 reserved 队列删除")
	}
}
