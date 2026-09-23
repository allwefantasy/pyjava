# PyJava Arrow 通信与压测 Review

后续修复与新测试结果见 [Spark/Ray 通信修复与验证](spark-ray-transfer-validation-2026-09-23.md)。本文保留修复前的审查结论与复现记录。

审查日期：2026-09-23。范围：本机 PyJava 工作区相对 `2dd0c1e` 的未提交改动、新增传输与压测文件、已有 RemoteService JSON 结果，以及 SQL 引擎中的实际调用位置。本轮没有修改产品代码、发布服务或重新执行远程压测。

**结论：现有工作可以作为协议冒烟和库级回归测试，不能作为“海量 Spark/Ray、Spark/Flink 数据交换已经稳定”的验收依据。** Python worker 池的改进有价值，但它不是跨引擎分区交换使用的连接池。当前仍有已复现的生命周期回归、压测误报成功，以及尚未解决的取消、数据完整性和重试问题。

**实际经过的两条路径**

| 用途 | 实际路径 | 本次连接池是否覆盖 |
|---|---|---|
| JVM 执行 Python 代码 | `ArrowPythonRunner → PythonWorkerFactory → daemon/worker` | Unix daemon 路径支持；池上限限制空闲 worker，不限制全部活动连接 |
| Spark 从 Ray 获取分区 | `RayDataServer/OnceServer → SparkSocketRunner.readFromStreamWithArrow` | 否，一次性监听、一次性读取 |
| Spark 向外提供分区 | `SparkSocketRunner.serveToStreamWithArrow → ArrowSocketServer` | 否，每次调用建立监听 Socket 和服务线程 |
| Flink 从 Spark 获取数据 | 本轮代码和测试中未找到 Flink connector 或 Flink 运行时验收 | 未覆盖，不能从前两条测试推导其可用性 |

[worker 池入口](/Users/williammacintel/projects/pyjava/src/main/java/tech/mlsql/arrow/python/PythonWorkerFactory.scala:60)、[Spark 分区监听入口](/Users/williammacintel/projects/pyjava/src/main/java/tech/mlsql/arrow/python/runner/ArrowSocketServer.scala:31)、[Ray 数据服务](/Users/williammacintel/projects/pyjava/python/pyjava/api/serve.py:114)。此外，当前 `releaseWorker` 只在 daemon 模式归还空闲池；之前“Windows/simple worker 也复用此池”的说明不适用于当前实现。

**需要优先处理的发现**

1. **P1，本轮回归：任务完成即关闭分区服务，破坏先收集地址、后消费数据的用法。**

   新 `ArrowSocketServer` 将监听口和已接受连接注册到任务完成回调。[注册位置](/Users/williammacintel/projects/pyjava/src/main/java/tech/mlsql/arrow/python/runner/ArrowSocketServer.scala:36)。仓库现有 [RayEnv.startDataServer](/Users/williammacintel/projects/pyjava/src/test/java/tech/mlsql/test/RayEnv.scala:124) 恰好是在 `mapPartitions` 中返回地址，`collect()` 后才让下游读取；此时 Spark task 已完成，地址失效。

   本轮用真正的 Spark 3.3 `local[1]` 和一行数据复现：`collect()` 成功，随后连接返回 `ConnectException`。已有 serve 测试在关闭手工创建的 `JavaContext` 前就读完，因此没有捕获此问题。

   影响边界：SQL 引擎 [MasterSlaveInSpark](/Users/williammacintel/projects/infinity-sql/streamingpro-mlsql/src/main/java/tech/mlsql/tool/MasterSlaveInSpark.scala:186) 会等服务关闭后才结束 task，不是同一个触发条件。修复应明确服务和分区数据的所有权：要么生产任务保持存活并与消费者协同调度，要么将可重读数据交给独立服务/暂存层；不能简单取消清理后永久保留已失效的迭代器。

2. **P1，本轮新增压测脚本缺陷：场景失败仍返回退出码 0。**

   [harness.main](/Users/williammacintel/projects/pyjava/python/pyjava/loadtest/harness.py:393) 捕获场景异常并返回 `1`，但模块入口 [__main__.py](/Users/williammacintel/projects/pyjava/python/pyjava/loadtest/__main__.py:3) 只调用 `main()`，未将返回值交给 `sys.exit`。远程脚本正是使用 `python -m pyjava.loadtest`，所以 `set -e` 无法识别这类失败。

   本轮设 `PYJAVA_ARROW_MAX_RECORDS_PER_BATCH=0`，只运行一个分区、一行数据。JSON 明确记录 `spark-ray failed 1 of 1 requests`，进程却退出为 `0`。应修正入口退出码，并让汇总与远程脚本显式校验成功数量和错误字段。这不证明以前那份 errors=0 的 JSON 是伪造的；它证明自动化任务可能把未来失败误判为成功。

3. **P1，现有路径仍未解决：取消和停止不能可靠终止已接受的连接。**

   [SparkSocketRunner](/Users/williammacintel/projects/pyjava/src/main/java/tech/mlsql/arrow/python/runner/SparkSocketRunner.scala:43) 默认无限读等待，只在任务完成回调关闭 Socket，没有 worker 路径那种主动观察中断的机制。本轮用真实 JVM TCP 连接复现：设置任务中断标志并 `Thread.interrupt()`，读线程仍阻塞；显式执行 context 完成清理后才退出。此探针验证了库级取消行为，尚未代替完整 Spark 集群取消验收。

   Python [OnceServer](/Users/williammacintel/projects/pyjava/python/pyjava/api/serve.py:53) 的 `close()` 仅关闭监听 Socket；接受后的 `conn` 为无限等待，并在发送完数据后等待 ACK。本轮读完数据但不发送 ACK，再调用 `server.close()`，服务线程仍阻塞；发一个错误 ACK `12345`，服务线程也会正常返回，因为代码没有验证 ACK 值。

   应将取消关联到活动连接，并区分建连、无进展、ACK 等待等期限。不能给整个大分区简单套一个很短的总超时。阻塞写也需要通过取消/无进展监测关闭连接，单设读超时不能覆盖。

4. **P1，现有协议缺口：Spark 导出的 Arrow 流在批次边界中断时，接收方可能接受不完整结果。**

   Spark 导出使用裸 Arrow IPC；[RayContext.fetch_data_from_single_data_server](/Users/williammacintel/projects/pyjava/python/pyjava/api/mlsql.py:411) 只遍历 Arrow reader，没有应用级分区完成记录、预期行数或摘要校验。服务端记录 `server.fail` 并不能直接让客户端确认完整性。

   本轮发送一个完整 record batch，去掉 Arrow 结束标记后关闭连接。真实 `RayContext.fetch_once_as_rows` 正常返回 16 行，没有错误。这不是声称 Arrow 解码器违反规范，而是说明“Arrow reader 正常迭代结束”不足以充当应用层完整分区提交证据。如果生产端恰好在批次边界失败，仅据此结束会留下静默截断风险。

   Ray → JVM 已有额外控制标记，其方向有所不同；不能把本发现不加区分地套到所有方向。Spark → Flink 也必须验证自己的完成语义，不能只检查能否打开 Arrow 流。

5. **P1，现有数据转换缺口：带空值的 int64 可能丢精度。**

   当前 Spark → Ray 默认行路径经过 `RecordBatch → Pandas → dict`，位置仍是 [mlsql.py](/Users/williammacintel/projects/pyjava/python/pyjava/api/mlsql.py:405)。本轮经真实 TCP 和该 helper 传输 Arrow int64 `[9007199254740993, null]`，得到 `9007199254740992.0` 和 `NaN`。Pandas 默认转换使这列成为浮点，超过精确整数范围的值已改变。

   这是原有路径的缺陷，不是本轮引入的代码回归，但它直接影响跨引擎数据正确性。现有测试全是较小整数，没有 null，因此不会发现。需要保留 Arrow 类型和 schema，行接口也应明确整数、null、Decimal、时区与嵌套类型的转换契约。

6. **P1，可靠性设计尚缺：一次性端点没有提供分区重读或任务重试契约。**

   [RayDataServer.serve](/Users/williammacintel/projects/pyjava/python/pyjava/api/serve.py:134) 服务一次后关闭并退出 actor。SQL 引擎 [Ray.scala](/Users/williammacintel/projects/infinity-sql/streamingpro-mlsql/src/main/java/tech/mlsql/ets/Ray.scala:195) 将返回的 host/port 放入 RDD，之后每个 task 连接同一地址。成功消费后若下游失败并重试，代码没有申请新端点、重新生成该分区或读取保留副本的流程。

   这是源码确认的恢复能力缺口，本轮未启动真实 Ray cluster 去做 executor/actor 故障注入。扩大连接池不能补上这一层；应先定义 partition/attempt 标识、数据保留与失效时间、重试和重复提交的处理。Flink 的恢复同样需要可重读输入与 checkpoint 对接，Socket ACK 本身不代表 checkpoint 提交。

7. **P2，本轮新增的并发资源风险：writer 线程的 Arrow 内存可被任务完成回调同时释放。**

   [writeLegacyArrowStream](/Users/williammacintel/projects/pyjava/src/main/java/tech/mlsql/arrow/ArrowConverters.scala:160) 用原子标志防止重复释放，但又注册任务完成回调直接 `root.close()/allocator.close()`；原子标志没有保护正在执行的写入或序列化。慢消费者、取消或任务提前完成时，存在另一线程仍在使用这些资源的窗口。同仓库 `ArrowPythonRunner` 的清理注释明确避免这种释放方式。

   此项为代码审查发现，未进行 JVM 崩溃级故障复现。建议取消路径先关闭 Socket 并通知 writer，由 writer 的 `finally` 释放自己持有的 Arrow 资源，同时对退出过程做有界等待。

**为什么 RemoteService 那轮不构成海量数据压测**

证据为 [远程结果 JSON](/Users/williammacintel/projects/pyjava/target/loadtest/remoteservice-report.json) 和 [harness.py](/Users/williammacintel/projects/pyjava/python/pyjava/loadtest/harness.py:311)。

| 检查项 | 实际行为 | 能支持的结论 |
|---|---|---|
| Spark/Ray/Flink 是否运行 | `spark-ray` 是同一 Python 进程中的 OnceServer 线程和 Python 客户端，全走 `127.0.0.1` | 部分 Python 线协议可读；不是实际引擎互通 |
| 数据量和时长 | 40 分区 × 20,000 行，单列 int64，共 800,000 行；约 6.4 MB 有效载荷，另有 IPC 元数据；耗时 1.178 秒 | 很小规模正常路径可通；没有持续负载证据 |
| worker/barrier 的 rows 参数 | 两个场景忽略 CLI 的 rows，实际请求默认一行并返回 PID | 小请求控制面测试 |
| 连接池覆盖 | Python harness 直接连接 daemon，绕过 JVM `PythonWorkerFactory` | 不能据此验收 JVM 池并发、过期、归还逻辑 |
| Barrier 语义 | 收到函数号即回 success；Scala 测试也用计数/报错 context | 回调可通；未证明真实 Spark barrier stage 的同步和失败传播 |
| 内存模型 | 发送端先构造完整 list；接收端 `read_all().to_pylist()` | 内存随分区大小增长，不能直接拿来验证有界流式传输 |
| 资源观测 | worker 场景只比 Python 主进程 FD 前后差；没有 daemon/worker 子进程 FD、RSS/Arrow 内存峰值 | 不能证明 worker 无泄漏，更不能证明长时间稳定 |
| 端口压力 | 每轮仅创建 workers 个服务，整轮消费完再开下一轮 | 未覆盖大量待消费分区、跨机器建连、TIME_WAIT 和调度延迟 |
| 故障和恢复 | 没有真实引擎任务失败、慢消费者、半开连接、actor/executor 退出、checkpoint 恢复 | 不能证明故障条件下可靠 |
| 性能改善 | 没有相同负载下的改前/改后对照 | 约 67.9 万行/秒是该 Python 小样本的观测值，不是跨引擎容量或性能提升比例 |

本次分批、去掉 JVM 导出的每批 `Array[Byte]` 中转、健康检查、显式清理等改动值得保留。但流式 TCP 自带的阻塞背压，不等于已经建立全链路内存预算。当前只按行数限制 batch，JVM child allocator 仍为 `Long.MaxValue`；例如 8192 行 × 64 KiB 有效数据就约 512 MiB/批，尚未计算 Pandas/dict 和其他副本。宽行、并发连接与慢消费者必须单独验证。

**与目标匹配的系统设计方向**

应将这套能力定义为跨引擎的“分区流服务”，明确数据所有权和恢复语义。worker 池继续负责执行 Python，分区数据服务单独管理。

每个 executor 或数据服务进程可以共享监听入口，以流 ID/分区 ID 定位数据；活动数据流设置数量上限和有界等待队列。共享监听口与每条活动流独占连接并不冲突；无论选择独占还是多路复用，都需要明确帧边界、流量控制和取消归属，不能把多个 Arrow 字节流直接混写。

批次同时受行数和字节预算约束，尽量保留 RecordBatch，避免默认经过 Pandas/dict 的重复转换。对需要重试的分区，明确可重算、保留内存块或受配额控制的磁盘暂存策略。协议应携带 schema、分区/尝试标识、完成或失败状态、行数/字节数校验以及可验证的结束确认。连接复用只能发生在上一条流完整结束以后。

目前 Spark 导出是裸 Arrow IPC，Python 回传则加控制标记；Flink reader 必须接入明确的协议和 schema 契约，不能直接假定 `SparkSocketRunner.readFromStreamWithArrow` 能读 Spark 自己导出的流。后者期望的是 Python 回传协议。

**建议重做的验收矩阵**

下面是待执行方案，不是已完成结果。数据规模应先根据 RemoteService 的可用 CPU、内存和其他服务负载设定资源预算；生成器和校验器必须流式运行，不以 `collect/read_all/list` 保存整份数据。

| 层次 | 实际参与者与负载 | 必须检查 |
|---|---|---|
| 正确性回归 | JVM ↔ Python；各方向协议；空分区、null、大整数、Decimal、时区、中文/二进制、嵌套和宽行 | schema、唯一行 ID、行数和分区摘要；截断必须失败 |
| 引擎集成 | 真正 Ray actor → Spark task；Spark executor → 真正 Flink reader/task | 下游 action/sink 实际消费；延迟调度、重复读取、task 完成时机 |
| 持续吞吐 | 独立进程；建议从 10 GiB 正确性运行逐步到 100 GiB 或更大总传输量，超过进程内存；暖机后每档持续测量 | 有效 GiB/s、线上字节、CPU、GC、堆/RSS/Arrow 内存峰值、首批和整分区延迟 |
| 分区和连接压力 | 分开测少量大分区与大量小分区；并发逐档增加，待消费分区数远大于执行槽数 | active/listen/TIME_WAIT、FD/线程/actor 峰值、排队时间、拒绝和超时原因 |
| 背压 | 消费者限速、暂停后恢复、提前结束读取 | 内存和队列保持预算内；上游停止推进；取消及时释放 |
| 故障恢复 | 中途断网、断在批次边界、Ray actor/Spark executor/Flink task 失败、分区重试 | 失败可见；数据不静默丢失；重试结果可校验且没有重复提交 |
| Flink 恢复 | 对接选定 connector 的 checkpoint/restart 流程 | 从已提交位置恢复，保留的数据和已确认偏移一致 |
| 耐久性 | 核心故障测试通过后再进行数小时持续负载 | 各进程资源随时间无持续爬升，停止后回落；故障率和恢复时间可追踪 |

单台 RemoteService 可以运行隔离的真实多进程集成测试，但仍不能声称验证了跨机器网络能力。网络容量需要至少两台机器的实际数据面；单机结果必须标记为单机。压测需使用独立目录、端口和明确 CPU/内存/磁盘配额，不能替换或重启正在运行的 SQL 引擎。性能对照应使用同一数据、并发、运行时版本与资源预算，对比改前/改后，并将 worker 小请求延迟和大分区吞吐分别报告。

**本轮验证与证据**

本轮复跑 `SparkSocketTransportSpec` 与 `SparkSocketServeSpec`，共 12 项，通过 12 项，无跳过。使用已有 Spark 3.3/Scala 2.12 编译产物；两个核心 Socket 源文件的修改时间早于对应 class。另行编译并执行了审查探针，其中端点生命周期探针使用真正的 Spark `local[1]`。没有重新跑 Spark 4.1、真实 Ray/Flink 集群或远程持续压测。

| 证据 | 内容 |
|---|---|
| [Socket 回归日志](/Users/williammacintel/projects/pyjava/target/review-20260923/existing-socket-tests.log) | 12 项现有测试通过 |
| [JVM 探针源码](/Users/williammacintel/projects/pyjava/target/review-20260923/TransportReviewProbe.scala) / [日志](/Users/williammacintel/projects/pyjava/target/review-20260923/jvm-probes.log) | 延迟消费者连接失败；取消/线程中断不解除阻塞读 |
| [Python 探针](/Users/williammacintel/projects/pyjava/target/review-20260923/repro_python.py) / [结果](/Users/williammacintel/projects/pyjava/target/review-20260923/python-probes.json) | 缺失终止标记仍被接受；关闭监听口不停止 ACK 等待；错误 ACK 被接受 |
| [故障压测 JSON](/Users/williammacintel/projects/pyjava/target/review-20260923/failing-loadtest.json) / [日志](/Users/williammacintel/projects/pyjava/target/review-20260923/failing-loadtest.log) | 明确场景失败，模块命令退出码实测为 0 |
| [整数精度探针结果](/Users/williammacintel/projects/pyjava/target/review-20260923/nullable-int64.json) | 含 null 的 int64 在真实 Python 接收 helper 中丢精度 |

`target/review-20260923` 是本轮可清理的验证产物。故障复现命令如下，预期应报告失败，但当前模块入口的进程退出码为 0：

```bash
cd /Users/williammacintel/projects/pyjava
PYTHONPATH="$PWD/python" PYJAVA_ARROW_MAX_RECORDS_PER_BATCH=0 \
  target/transport-venv/bin/python -m pyjava.loadtest \
  --scenario spark-ray --workers 1 --requests 1 --rows 1 \
  --output target/review-20260923/failing-loadtest.json
```

建议顺序：先修复可复现的生命周期和退出码问题，补上取消与完整性回归；再确定可重读分区和 Flink 接入契约；最后进行真实引擎的分级压测。现阶段不建议将现有压测结果作为海量交换能力的发布依据。
