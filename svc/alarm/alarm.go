package alarm

import (
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	OptionFunc func(options *option)
	option     struct {
		cachedTasks   int
		flushInterval time.Duration
	}

	Alarm struct {
		sender   *basicSender
		executor *executors.BulkExecutor
	}
)

var (
	options option
	alarm   *Alarm
	once    sync.Once
)

func SetUp(opts ...OptionFunc) *Alarm {
	once.Do(func() {

		alarm = &Alarm{
			sender: new(basicSender),
		}

		setSender(newLarkSender())

		for _, opt := range opts {
			opt(&options)
		}
		// apply ChunkExecutor options
		var bulkOpts []executors.BulkOption
		if options.cachedTasks > 0 {
			bulkOpts = append(bulkOpts, executors.WithBulkTasks(options.cachedTasks))
		}
		if options.flushInterval > 0 {
			bulkOpts = append(bulkOpts, executors.WithBulkInterval(options.flushInterval))
		}

		alarm.executor = executors.NewBulkExecutor(func(tasks []any) {
			for _, task := range tasks {
				if getSender() == nil {
					logx.Errorf("alarm.getSender is nil default config not set")
					return
				}
				if err := getSender().Send(task); err != nil {
					logx.Errorf("alarm.Send task data %v  error %v", task, err)
				}
			}
		}, bulkOpts...)
	})

	return alarm
}

func AppendSender(s Sender) {
	SetUp()
	os := alarm.sender.Load()
	if os == nil {
		setSender(s)
	} else {
		ocs, ok := os.(*comboSender)
		if ok {
			ocs.senders = append(ocs.senders, s)
		} else {
			ocs = &comboSender{
				senders: []Sender{s, os},
			}
		}
		setSender(ocs)
	}
}
func setSender(s Sender) {
	alarm.sender.Store(s)
}

func getSender() Sender {
	return alarm.sender.Load()
}

func Send(data any) error {
	SetUp()
	return alarm.executor.Add(data)
}
