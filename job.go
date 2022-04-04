package job

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"time"
)

type EventType int

const (
	EventTypeExecute EventType = iota
	EventTypeCancel
)

type ExecuteStatus int

const (
	//任务的状态
	TaskWaiting ExecuteStatus = iota //0，初始状态,等待运行
	TaskRunning
	TaskFinshed
)

type FinalState int

const (
	TaskSucceeded FinalState = iota
	TaskCanceled
	TaskTimeout
	TaskFailed
)

type Event struct {
	TaskId    string
	EventType EventType
}
type Task struct {
	// 任务的id
	Id string
	// 任务超时时间
	Timeout time.Duration
	Config  map[string]any
}
type TaskResult struct {
	Id               string
	Progress         *int
	ExecuteStatus    ExecuteStatus
	FinalState       FinalState
	FinalStateReason string
	logChan          chan string
	TimeStart        time.Time
	TimeEnd          time.Time
}

func (taskResult *TaskResult) Finish(finalState FinalState, finalStateReason string) {
	taskResult.FinalState = finalState
	taskResult.FinalStateReason = finalStateReason
	taskResult.ExecuteStatus = TaskFinshed
	*taskResult.Progress = 100
	taskResult.TimeEnd = time.Now()
}

func (taskResult *TaskResult) Append(line string) {
	taskResult.logChan <- line
}

type TaskJob struct {
	Id         string
	Task       *Task
	TaskResult *TaskResult
}

type TaskService interface {
	// 返回workerId
	Register(ctx context.Context, workInfo *WorkerInfo) error
	HeartBeat(ctx context.Context, workInfo *WorkerInfo) error
	Watch(ctx context.Context) (*Event, error)
	// Handle(ctx context.Context, event *Event) error
	GetTask(ctx context.Context, id string) (*Task, error)
	UpdateTask(ctx context.Context, taskJob *TaskJob) error
	AppendLog(ctx context.Context, id string, log string) error
}

type Consumer interface {
	Do(context.Context, *Task) error
}

type WorkerInfo struct {
	WorkerId  string
	WorkQueue string
	Version   string
	WorkerNum int
	//全局超时,优先使用任务设置的超时，然后是全局的。为0时代表全局不超时
	Timeout time.Duration
	// below do not need set
	LocalIP         string
	LastConnectTime time.Time
	WorkerDelay     time.Duration
}

// we use another struct(taskJobCtx) to store CancelFunc,  to avoid race conditions,@see https://go.dev/blog/race-detector
type taskJobCtx struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func (tj *taskJobCtx) cancel() {
	tj.cancelFunc()
}

type JobWoker struct {
	*WorkerInfo
	Consume         func(context.Context, *TaskJob) error
	TaskService     TaskService
	eventChan       chan *Event
	heartBeater     *WorkGroup
	informers       *WorkGroup
	handlers        *WorkGroup
	ctx             context.Context
	runningTasks    sync.Map
	runningTaskCtxs sync.Map
}

func (jw *JobWoker) Start(ctx context.Context) error {
	jw.ctx = ctx
	// TODO Check
	jw.LocalIP = getLocalIP()
	if err := jw.TaskService.Register(ctx, jw.WorkerInfo); err != nil {
		return fmt.Errorf("register failed %w", err)
	}
	jw.runningTasks = sync.Map{}
	jw.eventChan = make(chan *Event, jw.WorkerNum)
	jw.heartBeater = NewWorkGroup(ctx, "heartBeater", jw.heartBeat, 1)
	jw.heartBeater.Start()
	jw.informers = NewWorkGroup(ctx, "informer", jw.inform, 1)
	jw.informers.Start()
	jw.handlers = NewWorkGroup(ctx, "handle", jw.handle, jw.WorkerNum)
	jw.handlers.Start()
	return nil
}

func (jw *JobWoker) Stop() {
	jw.informers.Stop()
	jw.handlers.Stop()
	jw.heartBeater.Stop()
}

func (jw *JobWoker) heartBeat(ctx context.Context) {
	heartBeatFunc := func() {
		start := time.Now()
		if err := jw.TaskService.HeartBeat(ctx, jw.WorkerInfo); err != nil {
			log.Println(err.Error())
		}
		jw.WorkerDelay = time.Since(start)
		jw.LastConnectTime = start
	}
	heartBeatFunc()
	duration := time.Second * 5
	timer := time.NewTimer(duration)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			heartBeatFunc()
			timer.Reset(Jitter(duration, 0))
		}
	}
}

func (jw *JobWoker) inform(ctx context.Context) {
	var err error
	defer func() {
		if err != nil {
			log.Println(err.Error())
		}
	}()
	if event, err := jw.TaskService.Watch(ctx); err != nil {
		return
	} else if event != nil {
		jw.eventChan <- event
	}

}

func (jw *JobWoker) handle(ctx context.Context) {
	select {
	case event := <-jw.eventChan:
		taskId := event.TaskId
		if event.EventType == EventTypeExecute {
			defer jw.runningTasks.Delete(taskId)
			var progress int = 0
			taskJob := &TaskJob{
				Id: taskId,
				TaskResult: &TaskResult{
					Id:            taskId,
					Progress:      &progress,
					ExecuteStatus: TaskRunning,
					logChan:       make(chan string, 1024),
					TimeStart:     time.Now()},
			}
			jw.runningTasks.Store(taskJob.Id, taskJob)
			jw.executeTaskJob(ctx, taskJob)
		} else if event.EventType == EventTypeCancel {
			if v, ok := jw.runningTaskCtxs.Load(taskId); ok {
				v.(*taskJobCtx).cancel()
			}
		}
	default:
	}
}

func (jw *JobWoker) getTask(ctx context.Context, taskJob *TaskJob) bool {
	taskService := jw.TaskService
	taskId := taskJob.Id
	taskResult := taskJob.TaskResult
	task, err := taskService.GetTask(ctx, taskId)
	if task == nil && err == nil {
		err = errors.New("cannot find taskInfo for taskId: " + taskId)
	}
	if err != nil {
		taskResult.Finish(TaskFailed, "get task failed, "+err.Error())
		taskService.AppendLog(ctx, taskId, "get task failed: "+err.Error())
		taskService.AppendLog(ctx, taskId, "task faild")
		return false
	}
	taskJob.Task = task
	return true
}

func (jw *JobWoker) taskCtx(task *Task) (context.Context, context.CancelFunc) {
	//注意，不能使用parentCtx。因为parentCtx是本身worker节点的rootCtx传过来的：
	//如果worker程序停止，那么parentCtx会Done,会传递到正在执行的任务，要避免这种传递性。保证正在执行的任务结束
	if task.Timeout != 0 {
		//任务超时
		return context.WithTimeout(context.TODO(), task.Timeout)
	} else if jw.WorkerInfo.Timeout != 0 {
		//任务超时
		return context.WithTimeout(context.TODO(), jw.WorkerInfo.Timeout)
	} else {
		//任务不超时。取消
		return context.WithCancel(context.TODO())
	}
}

func (jw *JobWoker) executeTaskJob(parentCtx context.Context, taskJob *TaskJob) {
	taskService := jw.TaskService
	defer taskService.UpdateTask(parentCtx, taskJob)
	taskResult := taskJob.TaskResult
	if !jw.getTask(parentCtx, taskJob) {
		return
	}
	task := taskJob.Task
	ctx, cancel := jw.taskCtx(task)
	// store taskJobCtx
	jw.runningTaskCtxs.Store(taskJob.Id, &taskJobCtx{
		ctx:        ctx,
		cancelFunc: cancel,
	})
	defer jw.runningTaskCtxs.Delete(taskJob.Id)

	done := make(chan struct{})
	go func() {
		defer func() {
			cancel()
			done <- struct{}{}
		}()
		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					var errMsg string
					if _, ok := r.(string); ok {
						errMsg = fmt.Sprintf("Observed a panic: %s", r)
					} else {
						errMsg = fmt.Sprintf("Observed a panic: %#v (%v)", r, r)
					}
					err = errors.New(errMsg)
					taskResult.logChan <- errMsg
					const size = 64 << 10
					stacktrace := make([]byte, size)
					stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]
					taskResult.logChan <- string(stacktrace)
				}
			}()
			return jw.Consume(ctx, taskJob)
		}()
		ctxErr := ctx.Err()
		if err == nil {
			if ctxErr != nil { //对应这种情况：任务超时了或者发送了取消命令，但是Consume方法不支持取消。任务还是执行了成功了
				taskResult.logChan <- "consume do not support abandon,but finally finish successfully"
			}
			taskResult.Finish(TaskSucceeded, "succeed")
			taskResult.logChan <- "task succeeded"
		} else {
			if ctxErr == nil { //任务正常结束，发生了异常
				taskResult.Finish(TaskFailed, err.Error())
				taskResult.logChan <- "task failed"
			} else if errors.Is(ctxErr, context.Canceled) {
				taskResult.Finish(TaskCanceled, err.Error())
				taskResult.logChan <- "task canceled"
			} else if errors.Is(ctxErr, context.DeadlineExceeded) {
				taskResult.Finish(TaskTimeout, err.Error())
				taskResult.logChan <- "task timeout"
			}
			taskResult.logChan <- err.Error()
		}

	}()

	duration := time.Second
	timer := time.NewTimer(duration)

	for {
		select {
		case line := <-taskResult.logChan:
			taskService.AppendLog(ctx, task.Id, line)
		default:
			{
				select {
				//这边无法<-ctx.Done()，ctx在超时或者执行cancle方法时会立即收到信息，但是上面的goroutine还未结束
				//所以新搞了一个空结构体channel来监视结束信号
				case <-done:
					return
				case <-timer.C:
					{
						taskService.UpdateTask(ctx, taskJob)
						timer.Reset(Jitter(duration, 0))
					}
				}
			}
		}
	}

}

// Jitter returns a time.Duration between duration and duration + maxFactor *
// duration.
//
// This allows clients to avoid converging on periodic behavior. If maxFactor
// is 0.0, a suggested default value will be chosen.
func Jitter(duration time.Duration, maxFactor float64) time.Duration {
	if maxFactor <= 0.0 {
		maxFactor = 1.0
	}
	wait := duration + time.Duration(rand.Float64()*maxFactor*float64(duration))
	return wait
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
