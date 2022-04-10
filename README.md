# job-runner

[![Go](https://github.com/august0715/job-worker/actions/workflows/go.yml/badge.svg)](https://github.com/august0715/job-worker/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/august0715/job-worker/branch/main/graph/badge.svg)](https://codecov.io/gh/august0715/job-worker)

一个简单的原型，使用于`apiserver`-`agent`这种模式。

![arch](doc/img/arch.svg)

`apiserver`分发任务，`agent`指定`work_queue`来监听 `apiserver`的事件接口，获取事件之后并发执行。此架构属于`拉`模式。

此库主要用于实现`agent`端。

需要根据场景主要实现以下两点：

1. `TaskService`接口, 此接口是对`apiserver`的功能的抽象。

2. `JobWoker.Consume`，此方法是对任务具体的抽象，


框架实现了任务的执行、取消、超时、日志上报、优雅关闭等功能，这些都大量依赖`context.Context`。

所以本项目另外一个目的是能帮助大家彻底理解`context.Context`，知道其相关使用场景以及如何使用，当然`channel`也能更熟练。理解此项目就能熟练掌握`go并发编程`了。

demo见[job_test.go](job_test.go)

另外，apiserver端可以简单使用redis的brop实现，当然不考虑到分布式场景channel也可以。
