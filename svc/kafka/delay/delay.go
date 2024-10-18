package delay

import (
	_ "embed"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zhuud/go-library/svc/alarm"
)

const (
	tokenFormat     = "{%s}.tokens"
	timestampFormat = "{%s}.ts"
	pingInterval    = time.Millisecond * 100
)

var (
	//go:embed executablescript.lua
	tokenLuaScript string
	tokenScript    = redis.NewScript(tokenLuaScript)
)

func init() {
	go func() {
		if err := recover(); err != nil {
			logx.Errorf("consumer error: %v", err)
			_ = alarm.Send(alarm.LarkMessage{
				ReceiveType: "",
				ReceiveId:   "",
				MessageType: "",
				Content:     "",
			})
		}

	}()
}

func push() {

}

func pop() {
	// migrate delayed
	// migrate reserved
	// migrateExecutable

	//     $this->migrateExpiredJobs($queue.':delayed', $queue);
	//
	//    if (! is_null($this->retryAfter)) {
	//        $this->migrateExpiredJobs($queue.':reserved', $queue);
	//    }

}

func deleteReserved() {

}

func failed() {

}
