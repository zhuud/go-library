package internal

import (
	"context"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

var (
	ctx             = context.Background()
	timestamp int64 = 100
	r               = redis.MustNewRedis(redis.RedisConf{
		Host: "",
		Type: "node",
		Pass: "",
	})
)

func Test_Delete(t *testing.T) {
	SetUp(r, -1, "wechat")
	for i := 0; i < 10; i++ {
		r.Del(fmtQueueKey(int64(i), delayQueueName))
		r.Del(fmtQueueKey(int64(i), reservedQueueName))
	}
}

func Test_Delay(t *testing.T) {
	SetUp(r, -1, "wechat")

	err := Push(ctx, "100", map[string]interface{}{
		"hha": 17676,
		"sda": "sdfhfh",
	}, time.Second*10)
	spew.Dump(err)

	d, err := r.ZrangeWithScores(fmtQueueKey(timestamp, delayQueueName), 0, -1)
	spew.Dump(222, d)

	pd := Pop()
	spew.Dump(333, pd)

	d, err = r.ZrangeWithScores(fmtQueueKey(timestamp, delayQueueName), 0, -1)
	spew.Dump(444, d)
	d, err = r.ZrangeWithScores(fmtQueueKey(timestamp, reservedQueueName), 0, -1)
	spew.Dump(555, d)

	/*err = SuccessAck(pd[0])
	spew.Dump(666, err)
	d, err = r.ZrangeWithScores(fmtQueueKey(timestamp, reservedQueueName), 0, -1)
	spew.Dump(777, d, err)*/

	err = FailAck(pd[0])
	d, err = r.ZrangeWithScores(fmtQueueKey(timestamp, delayQueueName), 0, -1)
	spew.Dump(999, d)
	d, err = r.ZrangeWithScores(fmtQueueKey(timestamp, reservedQueueName), 0, -1)
	spew.Dump(1000, d)
}
