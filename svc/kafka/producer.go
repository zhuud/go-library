package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zhuud/go-library/utils"
)

type (
	produceMessage struct {
		Topic     any    `json:"topic"`
		From      string `json:"from"`
		Timestamp int64  `json:"timestamp"`
		Data      any    `json:"data"`
		LogId     string `json:"logid"`
	}
)

var (
	producers = map[string]*kq.Pusher{}
	mu        sync.Mutex
)

func Push(ctx context.Context, topic string, data any, sync ...bool) error {
	opts := []kq.PushOption{
		kq.WithChunkSize(10240),
		kq.WithFlushInterval(10 * time.Millisecond),
	}
	if len(sync) > 0 && sync[0] == true {
		opts = []kq.PushOption{
			kq.WithSyncPush(),
		}
	}
	producer, err := NewProducer(topic, opts...)
	if err != nil {
		return err
	}

	fileName := ""
	if _, file, line, ok := runtime.Caller(1); ok {
		fileName = fmt.Sprintf("%s:%d", file, line)
	}

	produce := &produceMessage{
		Topic:     topic,
		From:      fileName,
		Timestamp: time.Now().Unix(),
		Data:      data,
		LogId:     utils.TraceIDFromContext(ctx),
	}
	produceJson, err := json.Marshal(produce)
	if err != nil {
		logx.WithContext(ctx).Errorf("kafka.Push.Marshal data: %v, error: %v", produce, err)
		return err
	}

	return producer.Push(ctx, string(produceJson))
}

func NewProducer(topic string, opts ...kq.PushOption) (*kq.Pusher, error) {
	producer, ok := producers[topic]
	if ok {
		return producer, nil
	}

	mu.Lock()
	defer func() {
		proc.AddShutdownListener(func() {
			_ = producers[topic].Close()
		})
		mu.Unlock()
	}()

	servers, err := getServers()
	if err != nil {
		return nil, err
	}

	producer = kq.NewPusher(servers, topic, opts...)

	producers[topic] = producer

	return producers[topic], nil
}
