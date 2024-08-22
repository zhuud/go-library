package zookeeper

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zhuud/go-library/svc/conf"
)

type Client struct {
	conn *zk.Conn
	mu   sync.RWMutex
}

const defaultTimeOut = time.Second * 2

var (
	client             *Client
	connected          = int32(0)
	afterConnectedFunc []func()
	mu                 sync.RWMutex
)

func RegisterAfterConnected(handler func()) {
	mu.Lock()
	defer mu.Unlock()
	afterConnectedFunc = append(afterConnectedFunc, handler)
}

func NewZookeeperClient(servers ...string) (*Client, error) {
	if client != nil && client.conn != nil {
		return client, nil
	}

	mu.Lock()
	defer mu.Unlock()

	if len(servers) == 0 {
		if envServers, _ := conf.Get("ZK_ADDR"); len(envServers) > 0 {
			servers = strings.Split(envServers, ",")
		}
	}
	if len(servers) == 0 {
		_ = conf.GetUnmarshal("ZkAddr", &servers)
	}
	if len(servers) == 0 {
		return client, errors.New("zookeeper.NewZookeeperClient not set address, please set env ZK_ADDR or config ZkAddr")
	}

	zkConn, eventChan, err := zk.Connect(servers, defaultTimeOut,
		zk.WithLogInfo(false),
		zk.WithLogger(&loggerWrapper{}),
	)
	if err == nil {
		err = waitSession(eventChan, 10)
	}
	if err != nil {
		return nil, errors.Wrap(err, "zookeeper.NewZookeeperClient.Connect error")
	}
	client = &Client{conn: zkConn}

	return client, nil
}

func (z *Client) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	if z.conn == nil {
		return nil, nil, nil, errors.New("zookeeper conn is nil")
	}
	w, stat, events, err := z.conn.ChildrenW(path)
	if z.debugModeRetry(err) {
		return z.ChildrenW(path)
	}
	return w, stat, events, err
}

func (z *Client) Get(path string) ([]byte, *zk.Stat, error) {
	if z.conn == nil {
		return nil, nil, errors.New("zookeeper conn is nil")
	}
	get, stat, err := z.conn.Get(path)
	if z.debugModeRetry(err) {
		return z.Get(path)
	}
	return get, stat, err
}

func (z *Client) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	if z.conn == nil {
		return nil, nil, nil, errors.New("zookeeper conn is nil")
	}
	w, stat, events, err := z.conn.GetW(path)
	if z.debugModeRetry(err) {
		return z.GetW(path)
	}
	return w, stat, events, err
}

func (z *Client) Delete(path string, version int32) error {
	if z.conn == nil {
		return errors.New("zookeeper conn is nil")
	}
	err := z.conn.Delete(path, version)
	if z.debugModeRetry(err) {
		return z.Delete(path, version)
	}
	return err
}

func (z *Client) Exists(path string) (bool, *zk.Stat, error) {
	if z.conn == nil {
		return false, nil, errors.New("zookeeper conn is nil")
	}
	exists, stat, err := z.conn.Exists(path)
	if z.debugModeRetry(err) {
		return z.Exists(path)
	}
	return exists, stat, err
}

func (z *Client) Create(path string, data []byte, flag int32, acl []zk.ACL) (string, error) {
	if z.conn == nil {
		return "", errors.New("zookeeper conn is nil")
	}
	create, err := z.conn.Create(path, data, flag, acl)
	if z.debugModeRetry(err) {
		return z.Create(path, data, flag, acl)
	}
	return create, err
}

func (z *Client) Close() {
	if z.conn != nil {
		z.conn.Close()
	}
}

func (z *Client) State() zk.State {
	if z.conn != nil {
		return z.conn.State()
	}
	return zk.StateUnknown
}

func (z *Client) debugModeRetry(err error) bool {

	// 本地开发如果打了断点可能导致连接被中断，在发送下一次请求倩无法感知连接状态的变化，因此在此处针对这一场景进行重试
	if (errors.Is(err, zk.ErrNoServer) || errors.Is(err, zk.ErrConnectionClosed)) && z.conn.State() != zk.StateHasSession && conf.IsLocal() {
		b := time.Now()
		for {
			time.Sleep(time.Millisecond * 1)
			z.conn.State()
			if z.conn.State() == zk.StateHasSession {
				fmt.Printf("zookeeper connect retry(有可能是因为断点导致连接中断), time:%s, error:%v \n", time.Now().Sub(b).String(), err)
				return true
			}
		}
	}
	return false
}

func waitSession(eventChan <-chan zk.Event, retry int) error {
	for {
		event := <-eventChan
		switch event.State {
		case zk.StateHasSession:
			go func() {
				//如果因为程序hung住导致zk session过期（debug打断点也会出现类似的状况），zk会有重连的机制，
				//但是在重连后立即向zk发送任意指令，都会导致zkConn阻塞（原因未知，猜测是重连机制未完全执行完，此时是在往旧连接中发请求），
				//此时在操作系统中连接状态是WAIT_CLOSE或者CLOSE，但是go这边的状态还会一直是HasSession
				//实测延迟1秒再发送指令可以解决此问题
				time.Sleep(time.Second)
				mu.RLock()
				defer mu.RUnlock()
				for _, fun := range afterConnectedFunc {
					fun()
				}
				atomic.StoreInt32(&connected, 1)
			}()
			return nil
		case zk.StateConnecting:
			fmt.Printf("zookeeper waiting connect event:%s %s \n", event.State.String(), event.Server)
			retry--
			if retry < 0 {
				return errors.New("zookeeper.waitSession waiting connect error retry many times")
			}
		}
	}
}

func getConn() *zk.Conn {
	if client != nil && client.conn != nil {
		return client.conn
	}
	return nil
}

type loggerWrapper struct {
}

func (l *loggerWrapper) Printf(s string, i ...any) {
	logx.Infof("go-library: zookeeper: %s = %v", s, i)
}
