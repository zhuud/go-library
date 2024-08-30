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
		ReceiveIdType string
		ReceiveIdId   string
		MessageType   string
		Content       string
	}

	larkConfig struct {
		FSAppId     string
		FSAppSecret string
	}
	larkSender struct {
		client *lark.Client
	}
)

var (
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
	c := larkConfig{}
	err := conf.GetUnmarshal("Alarm", &c)
	if err != nil {
		logx.Errorf("")
	}

	client := lark.NewClient(c.FSAppId, c.FSAppSecret,
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
		return errors.New(fmt.Sprintf("alarm.larkSender.Send data doesn't match data: %v", data))
	}

	content, _ := json.Marshal(map[string]string{"text": msg.Content})
	resp, err := s.client.Im.Message.Create(context.Background(),
		larkim.NewCreateMessageReqBuilder().
			ReceiveIdType(msg.ReceiveIdType).
			Body(
				larkim.NewCreateMessageReqBodyBuilder().
					ReceiveId(msg.ReceiveIdId).
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
