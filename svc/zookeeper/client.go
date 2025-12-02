package zookeeper

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zhuud/go-library/svc/conf"
	"github.com/zhuud/go-library/svc/zookeeper/internal"
	"github.com/zhuud/go-library/utils"
)

type Client struct {
	conn    *zk.Conn
	mu      sync.RWMutex
	metrics *stat.Metrics
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

func MustNewZookeeperClient(servers ...string) *Client {
	c, err := NewZookeeperClient(servers...)
	if err != nil {
		log.Fatalf("zookeeper.MustNewZookeeperClient error: %v", err)
	}
	return c
}

func NewZookeeperClient(servers ...string) (*Client, error) {
	if client != nil && client.conn != nil {
		return client, nil
	}

	mu.Lock()
	defer mu.Unlock()

	var err error
	if len(servers) == 0 {
		servers, err = internal.GetServers()
		if err != nil {
			return nil, err
		}
	}

	zkConn, eventChan, err := zk.Connect(servers, defaultTimeOut,
		zk.WithLogInfo(false),
		zk.WithLogger(internal.NewZookeeperLogger()),
	)
	if err == nil {
		err = waitSession(eventChan, 10)
	}
	if err != nil {
		return nil, fmt.Errorf("zookeeper.NewZookeeperClient.Connect error: %w", err)
	}
	client = &Client{
		conn:    zkConn,
		metrics: stat.NewMetrics("zookeeper"),
	}

	proc.AddShutdownListener(func() {
		client.Close()
	})

	return client, nil
}

func (z *Client) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	startTime := utils.Now()
	conn := getConn()
	if conn == nil {
		z.recordMetrics(startTime, true)
		return nil, nil, nil, fmt.Errorf("zookeeper.ChildrenW zookeeper conn is nil")
	}
	w, state, events, err := conn.ChildrenW(path)
	if z.debugModeRetry(err) {
		return z.ChildrenW(path)
	}
	z.recordMetrics(startTime, err != nil)
	return w, state, events, err
}

func (z *Client) Get(path string) ([]byte, *zk.Stat, error) {
	startTime := utils.Now()
	conn := getConn()
	if conn == nil {
		z.recordMetrics(startTime, true)
		return nil, nil, fmt.Errorf("zookeeper.Get zookeeper conn is nil")
	}
	get, state, err := conn.Get(path)
	if z.debugModeRetry(err) {
		return z.Get(path)
	}
	z.recordMetrics(startTime, err != nil)
	return get, state, err
}

func (z *Client) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	startTime := utils.Now()
	conn := getConn()
	if conn == nil {
		z.recordMetrics(startTime, true)
		return nil, nil, nil, fmt.Errorf("zookeeper.GetW zookeeper conn is nil")
	}
	w, state, events, err := conn.GetW(path)
	if z.debugModeRetry(err) {
		return z.GetW(path)
	}
	z.recordMetrics(startTime, err != nil)
	return w, state, events, err
}

func (z *Client) Delete(path string, version int32) error {
	startTime := utils.Now()
	conn := getConn()
	if conn == nil {
		z.recordMetrics(startTime, true)
		return fmt.Errorf("zookeeper.Delete zookeeper conn is nil")
	}
	err := conn.Delete(path, version)
	if z.debugModeRetry(err) {
		return z.Delete(path, version)
	}
	z.recordMetrics(startTime, err != nil)
	return err
}

func (z *Client) Exists(path string) (bool, *zk.Stat, error) {
	startTime := utils.Now()
	conn := getConn()
	if conn == nil {
		z.recordMetrics(startTime, true)
		return false, nil, fmt.Errorf("zookeeper.Exists zookeeper conn is nil")
	}
	exists, state, err := conn.Exists(path)
	if z.debugModeRetry(err) {
		return z.Exists(path)
	}
	z.recordMetrics(startTime, err != nil)
	return exists, state, err
}

func (z *Client) Create(path string, data []byte, flag int32, acl []zk.ACL) (string, error) {
	startTime := utils.Now()
	conn := getConn()
	if conn == nil {
		z.recordMetrics(startTime, true)
		return "", fmt.Errorf("zookeeper.Create zookeeper conn is nil")
	}
	create, err := conn.Create(path, data, flag, acl)
	if z.debugModeRetry(err) {
		return z.Create(path, data, flag, acl)
	}
	z.recordMetrics(startTime, err != nil)
	return create, err
}

func (z *Client) Close() {
	conn := getConn()
	if conn != nil {
		conn.Close()
	}
}

func (z *Client) State() zk.State {
	conn := getConn()
	if conn != nil {
		return conn.State()
	}
	return zk.StateUnknown
}

func (z *Client) debugModeRetry(err error) bool {
	conn := getConn()

	// 本地开发如果打了断点可能导致连接被中断，在发送下一次请求倩无法感知连接状态的变化，因此在此处针对这一场景进行重试
	if (errors.Is(err, zk.ErrNoServer) || errors.Is(err, zk.ErrConnectionClosed)) && conn.State() != zk.StateHasSession && conf.IsLocal() {
		for {
			time.Sleep(time.Millisecond * 1)
			if conn.State() == zk.StateHasSession {
				log.Printf("zookeeper connect retry(有可能是因为断点导致连接中断) error: %v", err)
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
			log.Printf("zookeeper connected event:%s %s", event.State.String(), event.Server)
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
			log.Printf("zookeeper waiting connect event:%s %s", event.State.String(), event.Server)
			retry--
			if retry < 0 {
				return fmt.Errorf("zookeeper.waitSession waiting connect error retry many times")
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

// recordMetrics 记录监控指标
func (z *Client) recordMetrics(startTime time.Duration, drop bool) {
	if z.metrics == nil {
		return
	}
	st := stat.Task{
		Duration: utils.Since(startTime),
		Drop:     drop,
	}
	z.metrics.Add(st)
}
