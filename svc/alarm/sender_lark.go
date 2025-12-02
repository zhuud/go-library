package alarm

import (
	"context"
	"fmt"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/spf13/cast"
)

const (
	// defaultLarkTimeout 默认飞书请求超时时间
	defaultLarkTimeout = 5 * time.Second
)

type (
	// LarkMessage 飞书消息
	LarkMessage struct {
		ReceiveType string // 接收者类型（必填），如 "chat_id", "open_id"
		ReceiveId   string // 接收者 ID（必填）
		MsgType     string // 消息类型（必填），如 "text", "post", "image", "interactive" 等
		Content     string // 消息内容（必填）
	}

	// LarkConf 飞书配置
	LarkConf struct {
		AppId     string // 应用 ID
		AppSecret string // 应用密钥
	}

	// larkSender 飞书发送器
	larkSender struct {
		client *lark.Client
	}
)

// NewLarkSender 创建飞书发送器
func NewLarkSender(config LarkConf) Sender {
	// 创建飞书客户端
	client := lark.NewClient(
		config.AppId,
		config.AppSecret,
		lark.WithReqTimeout(defaultLarkTimeout),
		lark.WithLogLevel(larkcore.LogLevelError),
	)

	return &larkSender{
		client: client,
	}
}

// Send 发送飞书消息
func (s *larkSender) Send(data any) error {
	msg, ok := data.(LarkMessage)
	if !ok {
		return nil
	}

	// 检查必填字段
	if msg.ReceiveType == "" {
		return fmt.Errorf("alarm.larkSender ReceiveType is required")
	}
	if msg.ReceiveId == "" {
		return fmt.Errorf("alarm.larkSender ReceiveId is required")
	}
	if msg.MsgType == "" {
		return fmt.Errorf("alarm.larkSender MsgType is required")
	}
	if msg.Content == "" {
		return fmt.Errorf("alarm.larkSender Content is required")
	}

	// 发送消息
	resp, err := s.client.Im.Message.Create(
		context.Background(),
		larkim.NewCreateMessageReqBuilder().
			ReceiveIdType(msg.ReceiveType).
			Body(
				larkim.NewCreateMessageReqBodyBuilder().
					ReceiveId(msg.ReceiveId).
					MsgType(msg.MsgType).
					// 消息内容，json结构序列化后的字符串。不同msg_type对应不同内容。消息类型 包括：text、post、image、file、audio、media、sticker、interactive、share_chat、share_user等，具体格式说明参考：[发送消息Content](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json);;<b>请求体大小限制</b>：;- 文本消息请求体最大不能超过150KB;- 卡片及富文本消息请求体最大不能超过30KB
					//示例值：{\"text\":\"<at user_id=\\\"ou_155184d1e73cbfb8973e5a9e698e74f2\\\">Tom</at> test content\"}
					Content(cast.ToString(msg.Content)).
					Build(),
			).Build(),
	)
	if err != nil {
		return fmt.Errorf("alarm.larkSender failed to send message %w", err)
	}

	if resp.Code != 0 {
		return fmt.Errorf("alarm.larkSender lark api error %v", resp.CodeError)
	}

	return nil
}
