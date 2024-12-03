package alarm

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zhuud/go-library/svc/conf"
)

type (
	LarkMessage struct {
		ReceiveType string
		ReceiveId   string
		MessageType string
		Content     string
	}

	larkConfig struct {
		FSAppId         string
		FSAppSecret     string
		FsReceiveIdType string
		FsReceiveId     string
	}
	larkSender struct {
		client *lark.Client
	}
)

var (
	lc larkConfig
	ls *larkSender
)

func newLarkSender() Sender {
	if ls != nil {
		return ls
	}
	smu.Lock()
	defer smu.Unlock()

	level := larkcore.LogLevelInfo
	if conf.IsProd() {
		level = larkcore.LogLevelError
	}

	err := conf.GetUnmarshal(fmt.Sprintf("/qconf/web-config/%s", "alarm_wechat"), &lc)
	if err != nil {
		logx.Errorf("alarm.newLarkSender config cannot match error: %v", err)
		return nil
	}

	client := lark.NewClient(lc.FSAppId, lc.FSAppSecret,
		lark.WithReqTimeout(time.Second*5),
		lark.WithLogLevel(level),
	)

	return &larkSender{
		client: client,
	}
}

func (s *larkSender) Send(data any) error {

	msg, ok := data.(LarkMessage)
	if !ok {
		return errors.New(fmt.Sprintf("alarm.larkSender.Send data cannot match data: %v", data))
	}
	if len(msg.ReceiveType) == 0 {
		msg.ReceiveType = lc.FsReceiveIdType
	}
	if len(msg.ReceiveId) == 0 {
		msg.ReceiveId = lc.FsReceiveId
	}

	content, _ := json.Marshal(map[string]string{"text": msg.Content})
	resp, err := s.client.Im.Message.Create(context.Background(),
		larkim.NewCreateMessageReqBuilder().
			ReceiveIdType(msg.ReceiveType).
			Body(
				larkim.NewCreateMessageReqBodyBuilder().
					ReceiveId(msg.ReceiveId).
					MsgType("text").
					Content(string(content)).
					Build(),
			).Build(),
	)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return resp.CodeError
	}
	return err
}
