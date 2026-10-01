# Durable Workflow Orchestrator (local-only, Go)

一个**只依赖本地能力**（本地文件、进程内调度、标准库）的工作流编排引擎。
它保证一组有依赖关系的任务在**并发执行、失败重试、进程重启**后得到一致、
可解释的结果，无需任何外部服务（无数据库、无网络、无第三方 Go 依赖）。

- 按依赖 DAG 调度，支持扇出（fan-out）与扇入（fan-in）
- 全局并发上限 + 确定的调度顺序（同一输入每次执行顺序一致）
- 每任务：超时、最大尝试次数、fixed / linear / exponential 退避
- 可重试失败 vs 不可重试失败明确区分；失败后下游给出明确判定
- 整体取消 / 运行超时级联到正在运行与等待中的任务，不影响已完成结果
- 追加式、CRC 校验的本地日志（journal），崩溃后从断点恢复
- 部分写入 / 校验和损坏 / 状态不变量被破坏 / 定义被篡改：**拒绝**或**可显式修复**
- 单测打印输入、调度决策（DISPATCH / cascade / retry）与最终状态

---

## 1. 安装与本地验证

要求 Go 1.25+（仅使用标准库）。

```bash
# 全部单元测试（带竞态检测）
go test -race ./...

# 查看调度/重试/恢复决策日志
go test -race -v ./workflow/

# 端到端演示：真实多进程崩溃 -> 重启 -> 从 journal 恢复
go run . --reset --state-dir ./.wf-data
```

演示会：
1. 执行 6 个任务组成的 DAG（`prepare -> fetch-a/fetch-b -> enrich -> combine -> report`）；
2. `enrich` 前两次以**可重试**错误失败，第三次成功；
3. `combine` 第一次执行时**让自身进程退出（exit 42）**模拟硬崩溃；
4. 父进程重启子进程，引擎从本地 journal 恢复：已完成任务不重跑，
   崩溃时处于 `RUNNING` 的 `combine` 被重新执行；
5. 打印每个任务的**副作用次数（必须恰好 1 次）**；
6. 对已终态的 run 再调用一次，证明**幂等、零执行**。

运行产物都在 `--state-dir`：

```
<state-dir>/
  engine.lock          # 状态目录的排他文件锁（防止两个进程同时写）
  effects.log          # 演示用：任务副作用记录
  demo-run/
    journal.log        # 追加式、CRC32 保护的事件日志
  crashed.flag         # 演示用：模拟"是否已崩溃过"
```

---

## 2. 工作流定义格式

用 Go API 定义（也可用 JSON，字段 tag 与 `TaskDef` 一一对应）：

```go
wf, err := workflow.Build("demo-pipeline", []workflow.TaskDef{
    {ID: "prepare"},
    {ID: "fetch-a", Deps: []string{"prepare"}},
    {ID: "fetch-b", Deps: []string{"prepare"}},
    {
        ID:      "enrich",
        Deps:    []string{"fetch-a"},
        Timeout: workflow.Duration(2 * time.Second),
        Retry: workflow.RetryPolicy{
            MaxAttempts: 4,
            Backoff: workflow.BackoffPolicy{
                Kind:     workflow.BackoffFixed, // fixed | linear | exponential
                Interval: workflow.Duration(50 * time.Millisecond),
                MaxDelay: workflow.Duration(time.Second), // 可选上限
            },
        },
    },
    {ID: "combine", Deps: []string{"fetch-b", "enrich"}}, // 扇入
    {ID: "report",  Deps: []string{"combine"}},
})
```

### 字段

| 字段 | 含义 |
| --- | --- |
| `id` | 任务唯一标识，非空、不可重复 |
| `deps` | 依赖的任务 id 列表；必须全部存在、不能引用自己、不能重复、不能成环 |
| `timeout` | 单次尝试超时；`0` 表示不限时 |
| `retry.max_attempts` | 总尝试次数（含首次）；`<=1` 表示不重试 |
| `retry.backoff.kind` | `fixed` / `linear` / `exponential` |
| `retry.backoff.interval` | 退避基准时长（字符串如 `"50ms"`） |
| `retry.backoff.max_delay` | 单次退避上限 |
| `retry.backoff.multiplier` | 指数退避底数，默认 2 |
| `retry.backoff.jitter` | 在 `[d/2, d)` 内随机抖动（不影响结果，只影响时序） |

`Duration` 在 JSON 中写成字符串（`"250ms"`、`"3s"`）。

### 任务函数与失败分类

```go
type TaskFunc func(tc TaskContext) error
```

- 返回 `nil`：成功。
- 返回 `workflow.PermanentError(...)`：**不可重试**，立即 `FAILED`。
- 返回其它普通 error 或 `workflow.RetryableError(...)`：**可重试**，
  直到达到 `max_attempts`。
- 任务 panic：被捕获并按**不可重试**失败处理。
- `tc` 同时是 `context.Context`：取消/超时会关闭 `Done()`；
  任务应协作式响应（长任务请 `select <-tc.Done()`）。

### 非法定义：彼此可区分的拒绝原因

`Build` 返回 `*ValidationError`（可用 `errors.Is` 判定具体原因）：

| 哨兵错误 | 触发条件 |
| --- | --- |
| `ErrNoTasks` | 定义为空 |
| `ErrEmptyTaskID` | 存在空 id |
| `ErrDuplicateTask` | 任务 id 重复 |
| `ErrDuplicateDep` | 同一依赖边写了多次 |
| `ErrSelfDependency` | 任务依赖自己 |
| `ErrDanglingDep` | 依赖引用了不存在的任务 |
| `ErrCyclicDependency` | 依赖成环（错误信息列出环上任务） |
| `ErrInvalidRetryConfig` | 退避类型非法 / 负数等 |
| `ErrInvalidTimeout` | 超时为负 |

依赖校验（自依赖、悬空）在环检测**之前**进行，因此多个问题同时存在时
报告的原因是确定且可区分的。

---

## 3. 调度语义

- 任务在其**所有**依赖都 `SUCCEEDED` 后才进入就绪队列。
- 就绪队列按**定义顺序**排序（rank 升序）；调度器单 goroutine 做出派发
  决策，因此同一份输入**每次运行的任务启动顺序完全一致**
  （`TestSchedulingDeterministicAcrossRuns` 连续 5 次验证相同顺序）。
- `MaxWorkers` 是硬并发上限；超过上限的就绪任务排队等待。
- 每个任务**恰好派发一次直到终态**：成功/失败/跳过/取消后不会再次执行；
  不存在死锁、漏执行或重复执行（合法 DAG 下）。
- 扇入任务等待**全部**前置任务，扇出任务彼此并发。
- 状态目录持有 `flock` 排他锁，第二个进程/引擎同时打开会得到
  `ErrStoreLocked`，防止并发写坏同一份状态。

任务终态：

| 终态 | 含义 |
| --- | --- |
| `SUCCEEDED` | 某次尝试返回 nil |
| `FAILED` | 不可重试错误，或可重试错误耗尽 `max_attempts` |
| `SKIPPED` | 上游（非取消原因）失败/被跳过，数据依赖永不满足 |
| `CANCELED` | 运行被取消/超时，本任务未运行完或未运行 |

---

## 4. 失败与重试语义

- 尝试次数从 1 开始；`max_attempts=N` 时最多执行 N 次，第 N 次仍失败则
  该任务 `FAILED`。
- 退避（第 r 次失败后的等待，r 从 1 开始）：
  - `fixed`: `interval`
  - `linear`: `interval * r`
  - `exponential`: `interval * multiplier^(r-1)`（底数默认 2）
  - 均受 `max_delay` 上限约束；`jitter` 只在时间上引入随机性。
- **可重试 vs 不可重试**：普通 error / `RetryableError` 可重试；
  `PermanentError` 与 panic 不可重试（只执行 1 次）。
- 单次尝试 `timeout` 到期产生**可重试**的超时错误，受 `max_attempts` 约束。
- **下游判定**：任务 `FAILED` 后，其全部下游（递归）标记为 `SKIPPED`，
  错误信息形如 `downstream of X (FAILED): dependency can never succeed`；
  与其无依赖关系的分支**继续正常执行**（不会被连坐）。
- **整体取消 / 运行超时**：
  - `Engine.Run` 的 ctx 被取消，或 `Config.RunTimeout` 到期；
  - 正在运行的任务通过 ctx 收到取消并以 `CANCELED` 终态落盘；
  - 等待中的任务（含就绪队列里未派发的）全部级联为 `CANCELED`；
  - 已经 `SUCCEEDED/FAILED` 的任务结果**保持不变**；
  - 运行最终状态：有 `FAILED/SKIPPED` → `FAILED`；否则有 `CANCELED` 或
    ctx 结束 → `CANCELED`；全部成功 → `SUCCEEDED`。

---

## 5. 持久化与恢复规则

### 5.1 事件日志

每个 run 一个目录 `<StateDir>/<runID>/journal.log`，按行追加 JSON 信封：

```json
{"v":1,"crc":"a1b2c3d4","rec": { ...事件... }}
```

- `v`：信封版本，不支持的版本直接 `ErrJournalVersion` 拒绝。
- `crc`：`rec` 原始字节的 CRC32（IEEE），每条记录写入后 `fsync`。
- 事件类型：`RUN_STARTED`（内含整个定义的 SHA-256）、`TASK_STARTED`、
  `TASK_PROGRESS`（失败尝试观测）、`TASK_SUCCEEDED`、`TASK_FAILED`、
  `TASK_SKIPPED`、`TASK_CANCELED`、`RUN_FINISHED`。

`TASK_STARTED` 在**调度决策的瞬间、持调度锁**写入，因此：
- 持久化的事件顺序与确定性的派发顺序一致；
- 崩溃时最后一个处于 `STARTED` 但无终态事件的任务会被恢复为 `RUNNING`。

### 5.2 恢复规则（重启后对同一 runID 调用 `Run`）

1. 重放整个 journal，逐条校验版本、CRC、终结符；任何不一致**立即拒绝**，
   绝不静默继续。
2. 校验 `RUN_STARTED` 中保存的定义哈希；当前图与持久化不一致 →
   `ErrDefinitionMismatch`（防止改了图却复用旧状态）。
3. 强制状态机不变量，违反即 `ErrStateInconsistent`，例如：
   - `RUN_STARTED` 缺失/重复、终态后还有事件；
   - 对已终态任务再写 `STARTED/SUCCEEDED/...`；
   - 尝试序号回退；事件引用不存在的任务。
4. 由重放结果重建调度状态：
   - 已 `SUCCEEDED/FAILED/SKIPPED/CANCELED` 的任务：**不再执行**；
   - `RUNNING`（崩溃时在途）：重新执行该任务（**至少一次**语义；
     已完成任务绝不重复）；
   - `PENDING`：正常参与调度，**不会被跳过**；
   - 依赖计数器按持久化终态重算，恢复后失败任务的下游再次确定性地
     级联为 `SKIPPED/CANCELED`。
5. 若 `RUN_FINISHED` 已存在：直接返回持久化结论，**零任务执行**
   （重复调用安全、幂等，杜绝重复副作用）。

### 5.3 部分写入 / 损坏：拒绝或修复

- 末尾缺换行（torn append）、JSON 不合法、CRC 不匹配、未知版本：
  `Run` 返回 `ErrCorruptedJournal` / `ErrJournalVersion`，拒绝继续。
- 显式修复：`workflow.RepairCorruptedJournal(dir, runID)` 从第一条坏记录
  处截断（连同其后内容一并丢弃，因为后续记录不再可信），返回删除条数；
  修复后重新 `Run` 即从干净前缀继续。

### 5.4 副作用建议（at-least-once）

崩溃可能发生在"任务完成、终态记录落盘之前"，因此执行模型是
**至少一次**。要实现"恰好一次效果"，任务侧应让副作用幂等
（演示用 `effects.log` 去重；生产中可用外部去重键 / 幂等写入）。

---

## 6. API 速览

```go
eng, _ := workflow.NewEngine(workflow.Config{
    StateDir:   "./.wf-data",
    MaxWorkers: 4,
    RunTimeout: workflow.Duration(30 * time.Second), // 可选
    Logger:     workflow.NewWriterLogger(os.Stdout),
})
defer eng.Close()

eng.Register(wf, map[string]workflow.TaskFunc{
    "prepare": func(tc workflow.TaskContext) error { return nil },
    // ...每个 id 都必须提供函数，否则 Run 返回错误
})

res, err := eng.Run(ctx, "stable-run-id", wf)
// res.Status: SUCCEEDED | FAILED | CANCELED
// res.Tasks[id].Status / Attempts / Error / Retryable
```

要点：
- **run id 稳定**：用相同 run id 实现断点续跑；新 run id 开始全新运行。
- `Register` 的函数集合必须覆盖工作流全部任务。
- `Logger` 会打印输入（run/任务数/defhash/workers）、每次调度决策
  （DISPATCH、cascade、ATTEMPT_START/OK/FAIL、RETRY_WAIT）与最终状态表。

---

## 7. 代码结构

```
workflow/
  types.go      # 核心类型：TaskDef/TaskState/RunResult/策略/状态枚举
  errors.go     # 可区分的哨兵错误 + ValidationError
  taskerror.go  # PermanentError / RetryableError 与失败分类
  duration.go   # Duration 的 JSON 编解码（"2s"）
  validate.go   # Build：重复/悬空/自依赖/环(Kahn)/配置校验
  backoff.go    # fixed/linear/exponential 退避
  journal.go    # CRC 追加日志、重放、不变量、定义哈希、损坏修复
  lock.go       # 状态目录 flock
  engine.go     # 调度器：并发上限/重试/超时/级联取消/重启恢复
  logger.go     # 日志接口与默认实现
*_test.go       # 校验/调度/重试取消/恢复损坏/锁 的全部场景测试
main.go         # 本地崩溃-恢复端到端演示
```
