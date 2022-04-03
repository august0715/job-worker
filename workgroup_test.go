package job

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartRunner1(t *testing.T) {
	var count int32 = 0
	var count2 int32 = 0

	i := &count
	pn := rand.Intn(10)
	r := NewWorkGroup(context.Background(), "test", func(ctx context.Context) {
		time.Sleep(5 * time.Second)
		atomic.AddInt32(i, 1)
		count2++
		t.Log("end ", ctx.Value(WorkGroupIndexKey))
	}, pn)
	r.Start()
	time.Sleep(time.Second * 3)
	r.Stop()
	if int(count) != pn {
		t.Fail()
	}
}

func TestStartRunner2(t *testing.T) {
	var end int32 = 0
	i := &end
	ch := make(chan int32, 2)
	r := NewWorkGroup(context.Background(), "test", func(ctx context.Context) {
	l:
		for {
			select {
			case j := <-ch:
				atomic.AddInt32(i, j)
			case <-ctx.Done():
				break l
				// default:
			}
			// fmt.Println("123")
			time.Sleep(time.Microsecond * 10)
		}

	}, 4)
	r.Start()
	time.Sleep(time.Second * 5)

	var sum int32
	cc := rand.Intn(10)
	for i := 0; i < cc; i++ {
		n := int32(rand.Intn(100))
		sum += n
		ch <- n
	}
	r.Stop()
	if end != sum {
		t.Fail()
	}
}
