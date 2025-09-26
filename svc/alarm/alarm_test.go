package alarm

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/zeromicro/go-zero/core/proc"
)

func Test_Send(t *testing.T) {
	tt, _ := json.Marshal(LarkMessage{})
	AppendSender(newLarkSender())
	err := Send(LarkMessage{
		ReceiveType: "chat_id",
		ReceiveId:   "oc_53xxxxxxxxxxxxxx",
		Content:     fmt.Sprintf("hhh \n : %v", string(tt)),
	})
	proc.Shutdown()
	spew.Dump(err)
}
