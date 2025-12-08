package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

var (
	ctx = context.Background()
	r   = redis.MustNewRedis(redis.RedisConf{
		Host: "",
		Type: "node",
		Pass: "",
	})
)

func Test_Delay(t *testing.T) {
	DelaySetUp(r, 1000, "wechat")
	err := PushDelay(ctx, "5002", map[string]interface{}{"sss": 1}, time.Second*5)
	spew.Dump(err)
	time.Sleep(time.Second * 100)
}
