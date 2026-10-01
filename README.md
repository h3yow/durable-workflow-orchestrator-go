# Durable Workflow Orchestrator (local-only, Go)

一个**只依赖本地能力**的工作流编排引擎：按 DAG 依赖关系并发调度任务，支持失败
重试与退避、运行级/任务级超时与取消级联，并把运行状态持久化到本地磁盘，进程
重启后可以从断点继续，已完成任务不会重复执行、未完成任务不会被跳过。

无任何外部服务依赖（无数据库、无消息队列、无网络调用），持久化使用带校验和
与原子写的本地 JSON 文件。

## 1. 安装与要求

- Go 1.25+
- 仅使用标准库

```bash
go build ./...
go test -race -count=1 ./...
```

## 2. 工作流定义格式

定义可以直接用 Go 结构体构造，也可以从 JSON 文件加载
（`workflow.LoadDefinition("examples/workflow.json")`）。**任务的执行逻辑不
写在 JSON 里**——JSON 只描述拓扑与重试策略；任务行为在运行时通过
`workflow.Registry`（`map[任务ID]Executor`）注册。

```json
{
  "name": "etl-pipeline",
  "version": "1.0",
  "concurrency": 3,
  "run_timeout": "30s",
  "tasks": [
    { "id": "fetch",       "dependencies": [],
      "retry": { "timeout": "5s", "max_retries": 3,
                 "backoff": "exponential",
                 "initial_backoff": "100ms", "max_backoff": "2s" } },
    { "id": "validate",    "dependencies": ["fetch"] },
    { "id": "transform-a", "dependencies": ["validate"] },
    { "id": "transform-b", "dependencies": ["validate"] },
    { "id": "aggregate",   "dependencies": ["transform-a", "transform-b"] },
    { "id": "publish",     "dependencies": ["aggregate"] }
  ]
}
```

字段说明：

| 字段 | 含义 |
| --- | --- |
| `name` / `version` | 仅用于展示与运维区分。 |
| `concurrency` | 同时在执行的任务数上限（有界并发）。`<=0` 按 1 处理，大于任务数时按任务数处理。 |
| `run_timeout` | 整个运行的超时时间，字符串形式（`"500ms"`、`"2s"`、`"1m"`）；0 表示不限。 |
| `tasks[].id` | 任务唯一标识，**不允许重复、不允许为空**。 |
| `tasks[].dependencies` | 依赖的任务 ID 列表，**必须全部存在**；依赖关系不允许成环。任务会拿到所有上游成功任务的输出。 |
| `retry.timeout` | 单次尝试的超时；超时按**可重试失败**处理。 |
| `retry.max_retries` | 最大**重试**次数（不含首次），`2` 表示最多执行 3 次。 |
| `retry.backoff` | `fixed` / `linear` / `exponential`（默认 exponential）。 |
| `retry.initial_backoff` / `retry.max_backoff` | 退避起点与上限（指数：`init * 2^(attempt-1)`，封顶 `max_backoff`）。 |

任务函数签名：

```go
type Executor func(ctx context.Context, in TaskInput) (string, error)
```

- `in.Outputs`：所有上游成功任务的输出（`map[上游ID]输出`）。
- `in.Attempt`：当前尝试序号（从 1 开始）。
- `in.AttemptKey`：`<runID>/<taskID>/attempt-<n>`，用于实现幂等（见
  §5）。
- 返回字符串输出；输出会在扇入时传给下游。

### 2.1 非法定义被拒绝，且原因可区分

`ValidateDefinition` 返回的错误包装下列哨兵之一（用 `errors.Is` 判断），
并通过 `ValidationError.TaskID` 指出出问题的任务：

| 哨兵错误 | 触发条件 |
| --- | --- |
| `ErrDuplicateTask` | 任务 ID 重复（或同一依赖被声明两次）。 |
| `ErrUnknownDependency` | 依赖引用了不存在的任务。 |
| `ErrCyclicDependency` | 依赖成环（含自依赖）。 |
| `ErrMissingFunc` | 拓扑合法，但 `Registry` 中缺少该任务的执行函数。 |
| `ErrDefinitionMismatch` | Resume 时提供的定义与持久化时的定义哈希不一致。 |

## 3. 调度语义

- 拓扑排序决定可调度顺序，**同一层并列任务的调度次序按任务 ID 字典序**
  打破平局；在有界并发槽内，可运行任务不会饿死，不会漏执行，也不会重复
  执行（单测统计每个任务的实际执行次数并断言恰好一次）。
- 一个任务在其**全部依赖都为 `Succeeded`** 时才进入调度。
- 扇入：多上游任务的输出通过 `TaskInput.Outputs` 同时提供给汇聚任务。
- 并发由容量为 `concurrency` 的信号量限制；`-race` 下的测试验证了不会
  超过上限。
- 同一批输入重复运行，**终态结果是确定的**（任务状态、输出、失败链路一
  致）；物理上无关并发任务的启动先后不作为结果承诺。

### 任务终态

| 状态 | 含义 |
| --- | --- |
| `Succeeded` | 成功完成（终态）。 |
| `Failed` | 永久失败，或重试耗尽（终态）。 |
| `Skipped` | 上游出现 `Failed`/`Skipped`，该任务被判定为不会执行（终态）。 |
| `Canceled` | 因运行被取消/超时，或上游被取消而取消（终态）。 |
| `Pending` / `Ready` / `Running` | 中间态（等待 / 退避后就绪 / 执行中）。 |

## 4. 失败与重试语义

失败类型由执行器返回值显式区分：

- `workflow.Permanent(err)`：**不可重试**，立即 `Failed`（即使
  `max_retries > 0`）。
- `workflow.Retryable(err)`：**可重试**，按退避策略等待后重试。
- 任务 `timeout` 到期：等价于可重试失败。
- 普通（未包装）错误：按不可重试处理。

重试耗尽（或永久失败）后的下游判定：

- 失败任务的**全部传递下游**标记为 `Skipped`，原因写明
  `upstream task "X" did not succeed`；扇入场景下逐跳传播到不动点。
- **失败链路之外的无关任务继续执行**；运行整体状态为 `Failed`，但无关分
  支可以 `Succeeded`。

取消与超时级联：

- 外部 `context.Cancel` → 运行状态 `Canceled`；`run_timeout` 到期 →
  运行状态 `TimedOut`。两者通过 `context.WithCancelCause` 区分。
- 级联同时作用于**仍在运行**（执行器 ctx 被取消）和**仍在等待/退避**的
  任务：它们被标记为 `Canceled`，原因可区分（运行取消 vs 运行超时 vs
  上游取消）。
- **已经是终态的任务结果永不被覆盖**——已完成任务保持 `Succeeded`，失败
  任务保持 `Failed`。

## 5. 持久化与重启恢复规则

存储布局（`FileStore`）：

```
<dir>/runs/<runID>/manifest.json        # 运行清单 + 定义快照 + 定义哈希 + 校验和
<dir>/runs/<runID>/tasks/<taskID>.json  # 每个任务一条记录 + 校验和
```

- **原子写**：每次写入先落 `<file>.tmp`，`fsync` 后 `rename` 覆盖；进程
  崩溃不可能产生"写了一半的正式记录"。
- **校验和**：每条记录（manifest 与每个任务）都带 SHA-256 校验和，写入
  时重算。加载时逐条校验：JSON 损坏、字段被篡改、记录缺失、任务数与定义
  不符，都会返回包装 `ErrRunCorrupted` 的 `CorruptionError`（含 runID /
  taskID / 具体原因），**明确拒绝，绝不静默继续**。
- **启动修复**：打开存储时清理残留的 `.tmp` 文件（崩溃中断的原子写副产
  品），正式记录一律不动。
- manifest **只在运行干净到达终态时**才写成终态。

### Resume 规则（`Engine.Resume(runID, def, reg)`）

1. 先加载并校验全部记录：任何校验和/结构不一致直接返回
   `ErrRunCorrupted`，不执行任何任务。
2. 校验提供的定义与持久化的定义哈希一致，否则 `ErrDefinitionMismatch`。
3. 恢复判定：
   - `Succeeded` / `Failed` / `Skipped`：最终结论，**不会重新执行也不会
     被改写**；
   - `Running` / `Ready` / `Pending`：中断的尝试，重置为 `Pending` 后重新
     调度；
   - 非终态 manifest 下的 `Canceled` 记录只可能来自被硬杀掉的取消级联，
     同样回退为 `Pending`；
   - 终态 manifest 下的 Resume 是幂等的，直接返回已存储结果，不执行任务。
4. 重试计数 `attempts` 跨重启保留，因此重试上限在重启后依然有效。

### 幂等说明（重复副作用）

引擎对任务提供 **at-least-once** 保证：如果进程在"任务副作用已产生但
Running 记录尚未更新"之间崩溃，恢复时该次尝试会再跑一遍。执行器应当以
`TaskInput.AttemptKey`（`<runID>/<taskID>/attempt-<n>`）为幂等键自行去重
（例如先查本地结果文件/本地 KV 是否已有该 key 的产物）。状态记录本身不
会重复落库，重复的只会是崩溃窗口内的用户副作用。

## 6. 快速上手

```go
store, _ := workflow.NewFileStore("./wfdata")
eng := workflow.NewEngine(workflow.Options{Store: store})

def, _ := workflow.LoadDefinition("examples/workflow.json")
reg := workflow.Registry{
    "fetch": func(ctx context.Context, in workflow.TaskInput) (string, error) { ... },
    // ...每个任务一个实现
}
res, err := eng.Start(context.Background(), def, reg)      // 新运行
// 进程重启后：
res, err = eng.Resume(context.Background(), res.RunID, def, reg)
```

`Options.Logf` 是决策日志入口（默认 `log.Printf`），会打印输入定义、每次
`DISPATCH` / `RETRY` / `SKIP` / `CASCADE` 决策以及每个任务的终态，便于解
释"为什么执行/重试/跳过/取消"。

## 7. 本地验证方法

### 7.1 自带可运行 demo（无需写代码）

```bash
go run . run                 # 正常跑一条带重试的 DAG
go run . list                # 查看本地持久化的运行
go run . crash               # 在 validate 第 1 次尝试时模拟进程硬退出
go run . resume <runID>      # 重启后从断点继续（已完成的 fetch 不重跑）
go run . resume <runID>      # 再次 resume：终态幂等，不执行任何任务
```

数据落在 `./wfdata/`，可直接查看带校验和的 JSON。可以手工篡改
`wfdata/runs/<id>/tasks/*.json` 后再 `resume`/`list`，观察损坏被明确拒绝。

### 7.2 单元测试

```bash
go test -race -count=1 -v ./workflow
```

覆盖场景（测试均打印输入、调度决策日志与最终状态）：

| 测试 | 场景 |
| --- | --- |
| `TestValidateRejectsDistinctCauses` | 重复 ID / 未知依赖 / 自环 / 直接环 / 长环，错误原因彼此可区分。 |
| `TestHappyPathFanInFanOut` | 扇出 + 扇入汇聚，输出正确传递。 |
| `TestExactlyOnceAndDeterministicResults` | 每任务恰好执行一次、不超过并发上限、依赖顺序合法、重复运行终态指纹一致。 |
| `TestConcurrencyOneSerializes` | 并发上限=1 时严格串行。 |
| `TestRetryableThenSuccess` | 可重试失败后成功，attempts=3。 |
| `TestRetriesExhaustedDownstreamSkipped` | 重试耗尽→任务 Failed、传递下游 Skipped、无关分支 Succeeded。 |
| `TestPermanentFailureNoRetry` | 永久失败立即终止，不重试。 |
| `TestTaskTimeoutRetriedThenFailed` | 任务超时按可重试处理，耗尽后 Failed。 |
| `TestRunTimeoutCascades` | 运行超时级联：在跑/等待任务 Canceled，已完成任务保留。 |
| `TestExternalCancelCascades` | 外部取消级联语义同上，状态为 Canceled。 |
| `TestResumeAfterSimulatedCrash` | 构造硬崩溃残留状态（Succeeded/Running/Pending），Resume 后完成任务不重跑、中断任务续跑、下游不跳过。 |
| `TestResumeTerminalRunIsIdempotent` | 终态运行重复 Resume 不执行任务。 |
| `TestResumeRejectsDefinitionMismatch` | 定义变化被哈希拒绝。 |
| `TestResumeRejectsCorruption` | 篡改任务记录后 Resume 被 `ErrRunCorrupted` 拒绝。 |
| `TestStoreRejectsCorrupted*` / `TestStoreRepairsLeftoverTempFiles` | 校验和拒绝与 `.tmp` 修复。 |

## 8. 代码结构

```
workflow/
  types.go     定义、状态、结果类型
  errors.go    哨兵错误与 ValidationError / CorruptionError
  json.go      Duration 的 JSON 序列化（"2s" / 数字纳秒）
  validate.go  定义校验 + 确定性拓扑排序（含环检测）
  retry.go     Permanent/Retryable 包装与退避计算
  clock.go     可注入时钟（退避可测试）
  store.go     本地 FileStore：原子写、校验和、启动修复、损坏拒绝
  engine.go    Start / Resume 生命周期与恢复规则
  scheduler.go 单协调者 + 有界 worker 池：调度、传播、重试、取消级联
  loader.go    JSON 定义加载
main.go        本地 demo（run / crash / resume / list）
examples/workflow.json
```

## 9. 设计取舍与边界

- 单进程内调度；`FileStore` 面向"同一时刻一个进程写一个 run"。并发多进程
  写同一目录不在范围内（需要文件锁/租约）。
- 输出为字符串，适合做制品引用/摘要；大对象请由任务自行写到本地，用输出
  传递路径。
- 保证**结果一致、结论可解释、状态不丢不重（对状态而言）**；用户副作用
  的精确一次需要执行器基于 `AttemptKey` 幂等。
