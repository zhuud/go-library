package alarm

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/zeromicro/go-zero/core/proc"
)

func Test_Send(t *testing.T) {
	// 创建报警器实例（自动初始化飞书发送器）
	alarmInstance, err := New(
		WithLarkConfig(LarkConfig{
			AppId:     "xxxxxx",
			AppSecret: "xxxxxx",
		}),
	)
	if err != nil {
		t.Fatalf("failed to create alarm: %v", err)
	}

	// 发送文本消息（必须指定所有必填字段）
	err = alarmInstance.Send(LarkMessage{
		ReceiveType: "chat_id",
		ReceiveId:   "xxxxxx",
		MsgType:     "text",
		Content:     `{"text":"项目已更新"}`,
	})

	// 发送富文本消息示例
	postContent := `{"zh_cn":{"title":"项目更新通知","content":[[{"tag":"text","text":"项目已更新"}]]}}`
	err = alarmInstance.Send(LarkMessage{
		ReceiveType: "chat_id",
		ReceiveId:   "xxxxxx",
		MsgType:     "post",
		Content:     postContent,
	})
	proc.Shutdown()
	spew.Dump(err)
}
