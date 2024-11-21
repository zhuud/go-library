package alarm

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/zeromicro/go-zero/core/proc"
)

func Test_Send(t *testing.T) {
	tt, _ := json.Marshal(LarkMessage{})
	AppendSender(newLarkSender())
	err := Send(LarkMessage{
		ReceiveType: "chat_id",
		ReceiveId:   "oc_53b66a251e2a89ed74b4be3098262af5",
		Content:     fmt.Sprintf("hhh \n : %v", string(tt)),
	})
	proc.Shutdown()
	log.Println(err)
}
