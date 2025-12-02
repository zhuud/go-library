package alarm

import (
	"errors"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zhuud/go-library/svc/alarm/internal"
)

const (
	// DefaultCachedTasks 默认缓存任务数量
	DefaultCachedTasks = 100
	// DefaultFlushInterval 默认刷新间隔
	DefaultFlushInterval = time.Second
)

type (
	// Conf 报警器配置
	Conf struct {
		// CachedTasks 缓存任务数量，默认为 DefaultCachedTasks
		CachedTasks int
		// FlushInterval 刷新间隔，默认为 DefaultFlushInterval
		FlushInterval time.Duration
		// LarkConfig 飞书配置，如果不为 nil 则自动初始化飞书发送器
		LarkConfig *LarkConfig
		// 未来可扩展其他发送器配置
		// DingTalkConfig *DingTalkConfig
		// WechatConfig   *WechatConfig
	}

	// OptionFunc 配置函数
	OptionFunc func(config *Conf)

	// Alarm 报警器实例
	Alarm struct {
		config   *Conf
		sender   *internal.BasicSender
		executor *executors.BulkExecutor
		logger   internal.AlarmLogger
		mu       sync.RWMutex
	}
)

// New 创建一个新的报警器实例
func New(opts ...OptionFunc) (*Alarm, error) {
	// 默认配置
	config := &Conf{
		CachedTasks:   DefaultCachedTasks,
		FlushInterval: DefaultFlushInterval,
	}

	// 应用配置函数
	for _, opt := range opts {
		opt(config)
	}

	alarm := &Alarm{
		config: config,
		sender: internal.NewBasicSender(),
		logger: internal.NewAlarmLogger(),
	}

	// 初始化 sender
	if err := alarm.initSenders(); err != nil {
		return nil, err
	}

	// 初始化 executor
	alarm.initExecutor()

	return alarm, nil
}

// initSenders 初始化发送器
func (a *Alarm) initSenders() error {
	var senders []Sender

	// 根据配置自动初始化飞书发送器
	if a.config.LarkConfig != nil {
		senders = append(senders, NewLarkSender(*a.config.LarkConfig))
	}

	// 未来可扩展其他发送器
	// if a.config.DingTalkConfig != nil {
	//     senders = append(senders, NewDingTalkSender(*a.config.DingTalkConfig))
	// }
	// if a.config.WechatConfig != nil {
	//     senders = append(senders, NewWechatSender(*a.config.WechatConfig))
	// }

	// 设置发送器
	if len(senders) == 0 {
		return errors.New("alarm.initSenders no sender configured, please set LarkConfig or other sender config")
	}

	if len(senders) == 1 {
		a.sender.Store(senders[0])
	} else {
		a.sender.Store(internal.NewComboSender(senders))
	}

	return nil
}

// initExecutor 初始化执行器
func (a *Alarm) initExecutor() {
	var bulkOpts []executors.BulkOption

	if a.config.CachedTasks > 0 {
		bulkOpts = append(bulkOpts, executors.WithBulkTasks(a.config.CachedTasks))
	}
	if a.config.FlushInterval > 0 {
		bulkOpts = append(bulkOpts, executors.WithBulkInterval(a.config.FlushInterval))
	}

	a.executor = executors.NewBulkExecutor(func(tasks []any) {
		sender := a.sender.Load()
		if sender == nil {
			a.logger.Errorf("alarm.BulkExecutor sender is nil")
			return
		}

		for _, task := range tasks {
			if err := sender.Send(task); err != nil {
				a.logger.Errorf("alarm.BulkExecutor send failed, task: %v, error: %v", task, err)
			}
		}
	}, bulkOpts...)
}

// Send 发送报警消息（实例方法）
func (a *Alarm) Send(data any) error {
	if a.executor == nil {
		return errors.New("alarm.Send executor not initialized")
	}
	return a.executor.Add(data)
}

// Append 添加发送器（实例方法）
func (a *Alarm) Append(s Sender) {
	a.mu.Lock()
	defer a.mu.Unlock()

	currentSender := a.sender.Load()
	if currentSender == nil {
		a.sender.Store(s)
		return
	}

	// 如果已有 ComboSender，直接追加
	if cs, ok := currentSender.(*internal.ComboSender); ok {
		cs.Senders = append(cs.Senders, s)
		return
	}

	// 否则创建新的 ComboSender
	a.sender.Store(internal.NewComboSender([]Sender{currentSender, s}))
}

// ===== 配置选项函数 =====

// WithCachedTasks 设置缓存任务数量
func WithCachedTasks(tasks int) OptionFunc {
	return func(config *Conf) {
		config.CachedTasks = tasks
	}
}

// WithFlushInterval 设置刷新间隔
func WithFlushInterval(interval time.Duration) OptionFunc {
	return func(config *Conf) {
		config.FlushInterval = interval
	}
}

// WithLarkConfig 设置飞书配置
func WithLarkConfig(larkConfig LarkConfig) OptionFunc {
	return func(config *Conf) {
		config.LarkConfig = &larkConfig
	}
}
