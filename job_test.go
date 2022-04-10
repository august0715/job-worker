package job

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"
)

type testTask struct {
	id      string
	timeout time.Duration
	config  map[string]any
}

func (t *testTask) Id() string {
	return t.id
}

func (t *testTask) Timeout() time.Duration {
	return t.timeout
}

func TestJobWoker_Start(t *testing.T) {
	tasks := map[string]*testTask{
		"1": { //正常场景
			config: map[string]any{
				"cmd":        "date",
				"finalState": TaskSucceeded,
				"events":     []EventType{EventTypeExecute},
			},
		}, "2": { //异常
			config: map[string]any{
				"cmd":        "xxxxx",
				"finalState": TaskFailed,
				"events":     []EventType{EventTypeExecute},
			},
		}, "3": { //堆栈异常
			config: map[string]any{
				"cmd":        "panic error",
				"finalState": TaskFailed,
				"events":     []EventType{EventTypeExecute},
			},
		}, "4": { //超时
			timeout: time.Second * 2,
			config: map[string]any{
				"cmd":        "sleep 5",
				"finalState": TaskTimeout,
				"events":     []EventType{EventTypeExecute},
			},
		}, "5": { //超时无法取消
			timeout: time.Second * 2,
			config: map[string]any{
				"cmd":        "sleep 5",
				"p":          "1",
				"finalState": TaskSucceeded,
				"events":     []EventType{EventTypeExecute},
			},
		}, "6": { //取消
			config: map[string]any{
				"cmd":        "sleep 1000",
				"finalState": TaskCanceled,
				"events":     []EventType{EventTypeExecute, EventTypeCancel},
			},
		}, "7": { //无法取消
			config: map[string]any{
				"cmd":        "sleep 6",
				"p":          "1",
				"finalState": TaskSucceeded,
				"events":     []EventType{EventTypeExecute, EventTypeCancel},
			},
		}, "8": { //超时+取消,超时时间长，优先被取消
			timeout: time.Second * 10,
			config: map[string]any{
				"cmd":        "sleep 6",
				"finalState": TaskCanceled,
				"events":     []EventType{EventTypeExecute, EventTypeCancel},
			},
		}, "9": { //超时+取消,超时时间短，超时优先
			timeout: time.Second,
			config: map[string]any{
				"cmd":        "sleep 6",
				"finalState": TaskTimeout,
				"events":     []EventType{EventTypeExecute, EventTypeCancel},
			},
		},
	}
	taskResults := map[string]*TaskResult{}

	tc := func(ctx context.Context, taskJob *TaskJob[*testTask]) error {
		task := taskJob.Task
		if task.Id() == "3" {
			panic("panic error for 3")
		}
		c := task.config["cmd"]
		if _, useP := task.config["p"]; useP {
			ctx = context.TODO()
		}
		fmt.Println(c)
		cmd := exec.CommandContext(ctx, "sh", "-c", c.(string))
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("exec failed %w", err)
		}
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("exec failed %w", err)
		}
		return nil
	}
	eventCh := make(chan *Event)
	ts := &TaskService1{
		tasks:       tasks,
		eventCh:     eventCh,
		t:           t,
		taskResults: taskResults,
	}
	jw := &JobWoker[*testTask]{
		WorkerInfo: &WorkerInfo{
			WorkerId:  "1",
			WorkQueue: "test",
			Version:   "0.0.1",
			WorkerNum: 5,
		},
		Consume:     tc,
		TaskService: ts,
	}
	if err := jw.Start(context.TODO()); err != nil {
		t.Errorf("JobWoker.Start() error = %v", err)
	}
	for k, v := range tasks {
		// if k != "9" {
		// 	continue
		// }
		v.id = k
		config := tasks[k].config
		evs, _ := config["events"]
		evss := evs.([]EventType)
		for i, ee := range evss {
			if i > 0 {
				time.Sleep(time.Second * 3)
			}
			eventCh <- &Event{
				TaskId:    k,
				EventType: ee,
			}
		}
	}
	jw.Stop()
	// time.Sleep(time.Second * 15)
	for k, v := range taskResults {
		config := tasks[k].config
		if v.FinalState != config["finalState"].(FinalState) {
			t.Errorf("%s failed,expect state %d,really state %d", k, config["finalState"], v.FinalState)
		} else {
			t.Logf("%s successed,state %d", k, v.FinalState)
		}
	}
}

type TaskService1 struct {
	tasks       map[string]*testTask
	taskResults map[string]*TaskResult
	eventCh     chan *Event
	t           *testing.T
}

// 返回workerId
func (t *TaskService1) Register(ctx context.Context, workInfo *WorkerInfo) error {
	return nil
}
func (t *TaskService1) HeartBeat(ctx context.Context, workInfo *WorkerInfo) error {
	return nil

}
func (t *TaskService1) Watch(ctx context.Context) (*Event, error) {
	for {
		select {
		case e := <-t.eventCh:
			return e, nil
		case <-ctx.Done():
			return nil, nil
		}
	}

}

// Handle(ctx context.Context, event *Event) error
func (t *TaskService1) GetTask(ctx context.Context, id string) (*testTask, error) {
	r, _ := t.tasks[id]
	r.id = id
	return r, nil

}
func (t *TaskService1) UpdateTask(ctx context.Context, task *TaskJob[*testTask]) error {
	// fmt.Printf("task = %v", task)
	return nil

}
func (t *TaskService1) AppendLog(ctx context.Context, id string, log string) error {
	fmt.Printf("task[%s]:%s\n", id, log)
	return nil

}
