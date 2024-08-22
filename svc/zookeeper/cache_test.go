package zookeeper

import (
	"fmt"
	"testing"
)

func Test_GetC(t *testing.T) {
	z, err := NewZookeeperClient("10.10.10.10:2181")
	v, err := z.GetC("/db_wechat")
	fmt.Println(v, err)
}
