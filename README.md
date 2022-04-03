# job-runner

[![Go](https://github.com/august0715/job-worker/actions/workflows/go.yml/badge.svg)](https://github.com/august0715/job-worker/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/august0715/job-worker/branch/main/graph/badge.svg)](https://codecov.io/gh/august0715/job-worker)


一个简单的原型，使用于`apiserver`-`agent`这种模式。

`apiserver`分发任务，`agent`通过`Watch` `apiserver`的事件接口，来获取任务并发执行。

其中任务的定义支持取消、超时、日志上报等设置。

使用时实现`TaskService`接口即可，详情见[job_test.go](job_test.go)