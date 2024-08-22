package application

import (
    "fmt"
    "os"
    "os/signal"
    "sync"
    "sync/atomic"
    "syscall"

    "github.com/zeromicro/go-zero/core/logx"
)

var exitHandlers []func()
var exitHandlerMutex sync.Mutex
var exitHandlerExists = map[string]bool{} // 唯一约束
var exited int32                          // 是否已退出，如果退出则不在注册，直接执行
var running int32                         // 是否在运行中

// OnExit 注册退出处理函数，uniqKey为选填字段，如果传了会校验唯一性
func OnExit(handler func(), uniqKey ...string) {
    exitHandlerMutex.Lock()
    defer exitHandlerMutex.Unlock()
    if len(uniqKey) > 0 {
        if exitHandlerExists == nil {
            exitHandlerExists = map[string]bool{}
        }
        if exitHandlerExists[uniqKey[0]] {
            return //已注册过
        }
        exitHandlerExists[uniqKey[0]] = true
    }
    if atomic.LoadInt32(&exited) == 1 {
        //如果已退出或正在退出的过程中，则直接执行
        handler()
    } else {
        if exitHandlers != nil {
            exitHandlers = append(exitHandlers, handler)
        } else {
            exitHandlers = []func(){handler}
        }
    }
}

func WaitExit(handler ...func()) {
    c := make(chan bool, 1)
    OnExit(func() {
        if len(handler) > 0 {
            for _, h := range handler {
                h()
            }
        }
        c <- true
    })
    <-c
}

func Run(main func() error) (err error) {
    atomic.StoreInt32(&running, 1)
    sig := watchSignal() // 监听退出信号
    finish := run(main)  // 运行程序

    //程序结束或退出信号任意一个到达时
    select {
    case err = <-finish:
        fmt.Println("stop on finish")
    case <-sig:
        fmt.Println("stop on signal")
    }
    atomic.StoreInt32(&running, 0)
    //执行安全退出的业务
    safeExit()
    return err
}

func IsRunning() bool {
    return atomic.LoadInt32(&running) == 1
}

// run 开启协程执行程序
func run(main func() error) (finish chan error) {
    finish = make(chan error, 1)
    go func() {
        var err error
        defer func() {
            if r := recover(); r != nil {
                logx.ErrorStack(r)
            }
            finish <- err
        }()
        err = main()
    }()
    return finish
}

// watchSignal 等待退出信号
func watchSignal() chan os.Signal {
    sig := make(chan os.Signal, 1)
    signal.Notify(sig, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
    return sig
}

// safeExit 执行安全退出的业务逻辑
func safeExit() {
    if !atomic.CompareAndSwapInt32(&exited, 0, 1) {
        return
    }
    exitHandlerMutex.Lock()
    defer exitHandlerMutex.Unlock()
    if len(exitHandlers) == 0 {
        return
    }

    wg := sync.WaitGroup{}
    wg.Add(len(exitHandlers))
    for _, handler := range exitHandlers {
        //并行处理多个退出函数
        go func(handler func()) {
            defer func() {
                if r := recover(); r != nil {
                    logx.Error(r)
                }
                wg.Done()
            }()
            handler()
        }(handler)
    }
    wg.Wait()
}
