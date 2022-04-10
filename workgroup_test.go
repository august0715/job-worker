/*
Copyright 2022 github.com/august0715.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	i := &count
	pn := rand.Intn(10)
	r := NewWorkGroup(context.Background(), "test", func(ctx context.Context) {
		time.Sleep(5 * time.Second)
		atomic.AddInt32(i, 1)
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
