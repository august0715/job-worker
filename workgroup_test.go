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
	"sync"
	"sync/atomic"
	"testing"
)

func TestStartRunner1(t *testing.T) {
	var count int32 = 0
	i := &count
	pn := rand.Intn(100)
	runStates := sync.Map{}
	r := NewWorkGroup(context.Background(), "test", func(ctx context.Context) {
		atomic.AddInt32(i, 1)
		runStates.Store(ctx.Value(WorkGroupIndexKey), true)
	}, pn)
	r.Start()
	r.Stop()
	if int(count) < pn {
		t.Fail()
	}
	runStateCount := 0
	runStates.Range(func(key, value any) bool {
		runStateCount++
		return true
	})

	if runStateCount != pn {
		t.Fail()
	}
}

func TestStartRunner2(t *testing.T) {
	var end int32 = 0
	i := &end
	ch := make(chan int32, 20)
	r := NewWorkGroup(context.Background(), "test", func(ctx context.Context) {
	l:
		for {
			select {
			case j := <-ch:
				atomic.AddInt32(i, j)
			case <-ctx.Done():
				break l
			}
		}
	}, 4)
	r.Start()
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

func TestStartRunner3(t *testing.T) {
	var count int32 = 0
	i := &count
	pn := rand.Intn(100)
	runStates := sync.Map{}
	r := NewWorkGroup(context.Background(), "test", func(ctx context.Context) {
		atomic.AddInt32(i, 1)
		runStates.Store(ctx.Value(WorkGroupIndexKey), true)
		panic("unexpected error")
	}, pn)
	r.Start()
	r.Stop()
	if int(count) < pn {
		t.Fail()
	}
	runStateCount := 0
	runStates.Range(func(key, value any) bool {
		runStateCount++
		return true
	})

	if runStateCount != pn {
		t.Fail()
	}
}
