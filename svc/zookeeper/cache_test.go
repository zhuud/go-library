package zookeeper

import (
	"log"
	"testing"
)

func Test_GetC(t *testing.T) {
	z, err := NewZookeeperClient("10.10.10.10:2181")
	v, err := z.GetC("/db_wechat")
	log.Println(v, err)
}
