package job

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

const WorkGroupIndexKey = "workgroup_index"

// 因为：context的CancelFunc不会等待任务真正停止
// 所以有workerGroup的封装：workerGroup的stop方法，会同步等待任务真正stop
// 一个workerGroup对应一组goroutine，数量为parallelNum
type WorkGroup struct {
	name        string
	taskFunc    func(ctx context.Context)
	ctx         context.Context
	cancel      context.CancelFunc
	parallelNum int
	wg          sync.WaitGroup
}

func NewWorkGroup(
	parentCtx context.Context,
	name string,
	taskFunc func(ctx context.Context),
	parallelNum int) *WorkGroup {
	if parallelNum < 1 {
		panic("parallelNum must be positive")
	}
	ctx, cancel := context.WithCancel(parentCtx)
	r := &WorkGroup{name: name,
		taskFunc:    taskFunc,
		ctx:         ctx,
		cancel:      cancel,
		parallelNum: parallelNum,
		wg:          sync.WaitGroup{}}

	return r
}

func (worker *WorkGroup) Start() {
	fmt.Printf("workGroup [%s] starting", worker.name)
	for i := 0; i < worker.parallelNum; i++ {
		worker.wg.Add(1)
		worker.startOneGoroutine(i)
	}
	fmt.Printf("workGroup [%s] started", worker.name)
}

func (worker *WorkGroup) startOneGoroutine(i int) {
	go func() {
		defer worker.wg.Done()
		for {
			select {
			default:
				func() {
					defer HandleCrash()
					// 这个把goroutine的index放到ctx里
					worker.taskFunc(context.WithValue(worker.ctx, WorkGroupIndexKey, i))
				}()
			case <-worker.ctx.Done():
				return
			}
		}
	}()
}

// stop方法会等待taskFunc彻底退出
func (worker *WorkGroup) Stop() {
	fmt.Printf("workGroup [%s] stopping", worker.name)
	worker.cancel()
	// 等待真正停止信号
	worker.wg.Wait()
	fmt.Printf("workGroup [%s] stopped", worker.name)
}

var (
	// ReallyCrash controls the behavior of HandleCrash and now defaults
	// true. It's still exposed so components can optionally set to false
	// to restore prior behavior.
	ReallyCrash = true
)

// HandleCrash simply catches a crash and logs an error. Meant to be called via
// defer.  Additional context-specific handlers can be provided, and will be
// called in case of panic.  HandleCrash actually crashes, after calling the
// handlers and logging the panic message.
//
// E.g., you can provide one or more additional handlers for something like shutting down go routines gracefully.
func HandleCrash() {
	if r := recover(); r != nil {
		logPanic(r)
		if ReallyCrash {
			// Actually proceed to panic.
			panic(r)
		}
	}
}

// logPanic logs the caller tree when a panic occurs (except in the special case of http.ErrAbortHandler).
func logPanic(r any) {
	// Same as stdlib http server code. Manually allocate stack trace buffer size
	// to prevent excessively large logs
	const size = 64 << 10
	stacktrace := make([]byte, size)
	stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]
	if _, ok := r.(string); ok {
		fmt.Printf("Observed a panic: %s\n%s", r, stacktrace)
	} else {
		fmt.Printf("Observed a panic: %#v (%v)\n%s", r, r, stacktrace)
	}
}
