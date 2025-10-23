package server

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/zhuud/go-library/svc/server/internal"

	"github.com/go-zookeeper/zk"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zhuud/go-library/svc/conf"
	"github.com/zhuud/go-library/svc/zookeeper"
)

var (
	serverPath       string
	nodePath         string
	registerStopChan = make(chan bool, 1)
)

func MustRegisterZk() {
	err := RegisterZk()
	if err != nil {
		log.Panic(err)
	}
}

func RegisterZk() error {
	appName, err := conf.Get("Name")
	if err != nil || len(appName) == 0 {
		return fmt.Errorf("server.registerZk.getAppName error %w", err)
	}
	registerAddr, err := internal.GetRegisterAddr()
	if err != nil || len(registerAddr) == 0 {
		return fmt.Errorf("server.registerZk.getRegisterAddr error %w", err)
	}

	zkClient, err := zookeeper.NewZookeeperClient()
	if err != nil {
		return fmt.Errorf("server.registerZk.NewZookeeperClient error %w", err)
	}

	serverPath = fmt.Sprintf("/server_list/%s", appName)
	nodePath = fmt.Sprintf("%s/%s", serverPath, registerAddr)
	err = zkCreate(zkClient, serverPath, "", 0)
	if err != nil {
		return err
	}
	err = zkCreate(zkClient, nodePath, conf.AppZone(), zk.FlagEphemeral)
	if err != nil {
		return err
	}

	proc.AddWrapUpListener(func() {
		zkDelete(zkClient)
	})
	defer threading.GoSafe(func() {
		backgroundCheckZkNode(zkClient)
	})

	return nil
}

func zkCreate(zkClient *zookeeper.Client, path, data string, flag int32) error {
	exists, _, err := zkClient.Exists(path)
	if err != nil {
		return fmt.Errorf("server.zkCreate.Exists error %v", err)
	}
	if !exists {
		_, err = zkClient.Create(path, []byte(data), flag, zk.WorldACL(zk.PermAll))
		if err != nil && !errors.Is(err, zk.ErrNodeExists) {
			return fmt.Errorf("server.registerZk.Create error %w", err)
		}
	}
	log.Println(fmt.Sprintf("server.zkCreate success path %s", path))
	return nil
}

func zkDelete(zkClient *zookeeper.Client) {
	registerStopChan <- true
	if zkClient == nil {
		logx.Errorf("server.UnRegisterZk server %s  error zk is nil", nodePath)
		return
	}
	if err := zkClient.Delete(nodePath, 0); err != nil {
		logx.Errorf("server.UnRegisterZk.Delete server %s  error %v", nodePath, err)
		return
	}
	log.Println("server.zkDelete success")
}

func backgroundCheckZkNode(zkClient *zookeeper.Client) {
	if zkClient == nil {
		logx.Errorf("server.backgroundCheckZkNode node %s  error zk is nil", nodePath)
		return
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	//注册成功后每分钟检查一下临时节点是否存在，如果不存在就重新创建（OOM被杀掉后有可能在注册时节点存在，但过一会被zk删掉）
	for {
		select {
		case <-registerStopChan:
			log.Println("server.backgroundCheckZkNode stop check zk node")
			return
		case <-ticker.C:
			existsNode, _, err := zkClient.Exists(nodePath)
			if err != nil {
				logx.Errorf("server.backgroundCheckZkNode.Exists node %s  error %v", nodePath, err)
				continue
			}
			if existsNode {
				continue
			}
			logx.Infof("%v server.backgroundCheckZkNode node:%s recreate", time.Now().Local().Format(time.DateTime), nodePath)
			err = zkCreate(zkClient, nodePath, conf.AppZone(), zk.FlagEphemeral)
			if err != nil {
				logx.Errorf("server.backgroundCheckZkNode.zkCreate node %s  error %v", nodePath, err)
			}
		}
	}
}
