# Spark/Ray Arrow 通信修复与验证

日期：2026-09-23。基于 PyJava 工作区已有改动继续修复，尚未发布 Maven/PyPI，也未替换 RemoteService 上运行中的 SQL 引擎。修复前的问题见 [Review](pyjava-transport-review-2026-09-23.md)。

这次补上了分区生命周期、取消、截断检测、整数精度、有限期重读和批次字节预算，并用真正的 Spark task 与 Ray actor 做往返和失败重试。原 Python `spark-ray` 场景仍保留为协议冒烟测试；它不再被当作真实引擎压测。

## 数据路径与生命周期

```mermaid
flowchart LR
    S[Spark 生产任务] -->|分批写 Arrow| D[有配额的分区临时文件]
    D -->|一次性 TCP 流| R[Ray actor / Arrow 批次变换]
    R -->|分批写 Arrow| O[有配额与有效期的输出快照]
    O -->|TCP 流| T[Spark 消费任务]
    T -->|失败后重新连接同一端点| O
```

- **Spark 直接流式提供分区**：默认沿用任务所有权。生产任务必须存活到消费完成；任务结束、取消会关闭端点。
- **先收集地址，再读取分区**：调用 `serveToStreamWithArrow` 时传入 `python.socket.detached=true`。在生产任务中先把迭代器完整写成 Arrow 文件，再返回服务地址。后台线程只读取已提交文件，不再持有已经结束的 Spark task 的迭代器。消费一次或等待超时后删除文件。仓库 `RayEnv` 示例已使用该模式。
- **Ray 输出**：先生成有配额的完整快照，再向 Spark 提供流。Spark task 失败后可在租期内重新连接，从分区起点读取同一份数据；已经运行过的变换不会因这次读取重试再执行。每次读取使用独立连接，每个 actor 顺序处理读取请求。

这不是 actor/节点退出后的持久恢复，也不是从任意字节偏移续传。Spark 导出端仍消费一次；Ray 输出端才提供租期内多次读取。没有增加全局共享监听口或跨分区 Socket 池，每个分区 actor 仍有独立监听端点。

## 修复内容

| 问题 | 当前处理 |
| --- | --- |
| `collect()` 返回地址时生产任务已经结束 | 明确区分直接流式与 detached 快照；后台服务不再使用失效的 Spark 迭代器 |
| 任务取消后阻塞读不退出 | JVM 监测任务状态并关闭 Socket；Python `close()` 同时关闭监听和活动连接 |
| 任务回调与 writer 并发释放 Arrow 内存 | 取消先停止 Socket，writer 在自己的 `finally` 中释放 Arrow；reader 清理与读取互斥 |
| 无进展读写或 ACK 永久等待 | 分开设置连接、接受、数据 I/O 与 ACK 等待期限；没有给整个大分区套固定总时长 |
| 批次边界截断被当成正常结束 | Python 接收 Spark 数据时强制读取 IPC 结束标记；短读报错 |
| nullable int64 经 Pandas 转浮点丢精度 | 默认行接口从 Arrow 列直接构造 Python 行；增加原生 `map_batches` 接口 |
| 行字段变化被 Arrow 推断静默忽略 | 从第一行开始检查字段集合，分区中字段/schema 改变明确失败 |
| Spark task 重试时 Ray 端点消失 | Ray 保留已提交快照至租期结束，允许新连接完整重读；ACK 错误不算成功 |
| 只按行数分批，宽行占用失控 | Spark/Ray 输出同时限制行数、批次数据字节；超大单行失败，JVM Arrow allocator 有上限 |
| 压测失败但进程退出码为 0 | 模块入口使用 `sys.exit(main())`；测试实际验证失败时退出 1 |
| 压测整分区 `list/read_all`，请求数和行数不准确 | harness 改为增量生成/校验，正确处理余数与较少请求，父进程 FD 指标明确标注为父进程 |

保留了既有 Arrow IPC 与控制标记，未增加自动重放用户变换的逻辑。对外接收原始 Spark Arrow 的第三方客户端仍须自行严格检查结束语义。

## 大数据调用方式

已创建并配置好的 `RayContext` 可以直接处理 Arrow 批次：

```python
def transform(batches):
    for batch in batches:
        # 在这里使用 Arrow 列式操作；输出 RecordBatch 或 Table。
        yield batch

ray_context.map_batches(transform)
```

`map_iter` 和逐行接口仍可用，但会生成 Python 行对象。明确请求 DataFrame 的接口仍使用 Pandas，其类型转换语义没有被改成 Arrow。需要完整保留 Decimal、时区、嵌套类型和空分区 schema 时，优先输出显式类型的 Arrow 批次；本轮没有完成这些所有类型组合的跨引擎验收。

Spark 延迟消费示例：

```scala
val endpoint = runner.serveToStreamWithArrow(
  rows, schema, 10000, taskContext,
  Map(
    "python.socket.detached" -> "true",
    "python.socket.spool.maxBytes" -> "2147483648",
    "python.socket.accept.timeout" -> "300000"
  )
)
// 此时 rows 已在生产任务内消费完毕，endpoint 不再依赖该迭代器。
```

快照策略增加完整分区写盘和首批等待时间，换取任务生命周期独立和结果可重读。它不等于零拷贝，也不能消除下游排队。若生产任务可以持续存活，直接流式路径仍可避免这一份 Spark 暂存。

## 默认预算与期限

| 配置 | 默认值 | 作用范围 |
| --- | --- | --- |
| `python.arrow.maxBytesPerBatch` | 8 MiB | SparkSocketRunner 输出批次的 Arrow vector 数据；IPC 元数据另计 |
| `python.arrow.maxAllocation` | 128 MiB | 每个 SparkSocketRunner reader/writer 的 Arrow allocator |
| `python.socket.detached` | `false` | Spark 分区导出是否先提交快照 |
| `python.socket.spool.maxBytes` | 1 GiB | 单个 detached Spark 分区快照 |
| `python.socket.accept.timeout` | 300000 ms | Spark 导出端等待首个消费者 |
| `python.socket.read.timeout` | 300000 ms | SparkSocketRunner 读取；0 禁用，取消监测仍生效。worker 路径默认仍为 0 |
| `python.socket.write.timeout` | 300000 ms | Spark 导出无进展写入；0 禁用 |
| `PYJAVA_ARROW_MAX_RECORDS_PER_BATCH` | 8192 | Python/Ray 输出批次行数 |
| `PYJAVA_ARROW_MAX_BYTES_PER_BATCH` | 8 MiB | Python/Ray 输出批次数据字节 |
| `PYJAVA_SPOOL_MAX_BYTES` | 1 GiB | 单个 Ray 输出快照，含 IPC 数据 |
| `PYJAVA_SPOOL_DIR` | 系统临时目录 | Ray 快照目录；文件在关闭后删除 |
| `PYJAVA_REPLAY_TTL_SECONDS` | 300 s | Ray 完成物化后开始计算的接受新 reader 租期 |
| `PYJAVA_SOCKET_TIMEOUT_SECONDS` | 300 s | Python 数据 Socket I/O |
| `PYJAVA_ACCEPT_TIMEOUT_SECONDS` | 300 s | Python 一次性服务接受连接 |
| `PYJAVA_ACK_TIMEOUT_SECONDS` | 30 s | Python 服务等待 Spark ACK |

Spark 导出行数上限仍由 `maxRecordsPerBatch` 参数指定。上述都是每个批次、连接或分区的限制，不是整台机器的总预算。单行超过批次预算直接报错；单分区超过暂存预算也明确失败，需要在调用端合理分区或显式提高配额。

Ray 租期从快照完成开始计时；需要覆盖 Spark 调度延迟和允许的重试窗口。已经接纳的流可以在持续取得进展时超过租期，后续新连接不再被接纳。高并发部署还需要按可用磁盘和内存限制 actor、待消费分区数量；本轮未实现主机级总配额和全局准入队列。

## 已执行验证

### 本机回归

- 28 个 Python 测试通过，包含真实 TCP、nullable int64、截断、ACK、取消、快照重读、字节配额、schema 变化及压测退出码。
- Spark 3.3 / Scala 2.12：50 个 JVM 测试通过；1 个可选延迟 benchmark 未启用。新增真实 Spark 生产任务结束后读取、取消阻塞 reader、宽行按字节分批与 allocator 回收测试。
- 本机版本：JDK 17、Python 3.9.6、PyArrow 18.1.0、Arrow Java 7.0.0。日志：`target/transport-validation/final-python.log`、`final-spark33.log`。

### RemoteService 真实引擎

每次运行创建独立目录和自己拥有的 Ray cluster，使用现有 Spark 运行时的只读 jars，限定 Spark 2 个本地执行槽、Ray 2 CPU、JVM `-Xmx1g`、直接内存 512 MiB。运行后关闭自己创建的进程。没有向现有 Ray cluster 提交测试，也没有重启 SQL 引擎。

实际链路为 Spark 生成两个分区并提交快照 → 真正的 Ray actor 从 Socket 读取并变换 → 真正的 Spark task 从 Ray 读取。Spark driver 只收集两个端点和校验计数，不收集整份数据。每行检查全局 ID、含 null 的大整数以及完整字符串内容；第一轮故意让一个 Spark task 读过 100 行后失败，由 Spark 重试并重新读取成功。

远程版本：Python 3.10.12、Ray 2.47.1、PyArrow 23。Spark 3.3 使用 JDK 8；Spark 4.1.2 使用 JDK 17 / Scala 2.13.17 / Arrow Java 18.3.0。

| 场景 | 每分区行数 / 字符串宽度 | 成功读取轮数 | 累计逻辑返回量 | 消费阶段时间 | 结果 |
| --- | --- | --- | --- | --- | --- |
| Spark 3.3 原生批次 | 20,000 / 256 B | 2 | 21.76 MB | 0.212 s | 逐行验证和任务重试通过 |
| Spark 3.3 行接口（最终代码） | 20,000 / 256 B | 2 | 21.76 MB | 0.200 s | nullable 大整数精确，重试通过 |
| Spark 3.3 较大分区 | 600,000 / 1024 B | 2 | 2.496 GB | 4.640 s | 逐行验证和任务重试通过 |
| Spark 3.3 连续重读 | 600,000 / 1024 B | 100 | 124.8 GB | 98.667 s | 1.2 亿行全部校验通过 |
| Spark 4.1.2 原生批次 | 20,000 / 256 B | 2 | 21.76 MB | 0.254 s | 主源码编译、真实往返和任务重试通过 |

“逻辑返回量”按 `行数 × 2 分区 × 读取轮数 × (字符串宽度 + 16)` 估算，不含 Arrow 元数据/空值位图，也不是抓包测得的网络字节数。消费阶段含第一次等待 Ray 输出和注入重试；不含前面的 Spark 输入物化与 Ray 启动。100 轮整套测试主程序耗时 107.617 秒。

100 轮测试重复读取同两个快照，唯一输入约 1.248 GB。数据在同机进程间传输并受操作系统页缓存影响；约 1206 MiB/s 是该合成负载的逻辑读取观测值，不代表跨机器网卡能力，也不是修改前后的性能提升比例。未运行数小时耐久性测试。

连续重读时每轮 action 完成后，Arrow 根 allocator 占用均为 0，JVM FD 数均为 318。JVM RSS 采样峰值 688,885,760 字节（约 657 MiB），进程树 RSS 求和峰值 1,774,964,736 字节；后者重复计算共享页，不能当作 PSS。200 ms 采样可能漏掉短暂尖峰。

| 证据目录（项目根目录下） | 内容 |
| --- | --- |
| `target/engine-validation/pyjava-validation.hjdH7YFV/` | Spark 3.3 原生批次小规模 |
| `target/engine-validation/pyjava-validation.GWTyMj0q/` | 最终行接口代码复测 |
| `target/engine-validation/pyjava-validation.kDI3JXJ2/` | 较大分区，两次重读 |
| `target/engine-validation/pyjava-validation.Xd1EaAZQ/` | 100 轮、资源峰值和 `per-pass.csv` |
| `target/engine-validation/pyjava-spark41.Au3A50bu/` | Spark 4.1 主源码编译和真实引擎测试 |

100 轮之后另补了首批行字段变化与超大 RecordBatch 分片深递归的 Python 防护，并用旧版 PyArrow 已有的列构造 API 实现行转换，避免依赖较新版本才提供的 `RecordBatch.from_pylist`。最终 Python 回归和真实引擎复测见日志。JVM 主代码在 100 轮之后没有变化。PyArrow 4.x 本身没有在本轮单独运行，不能据此宣称其完整兼容性已经验收。

Spark 4.1 的完整 Maven suite 因依赖下载超时未完成；以上 4.1 结果来自在 RemoteService 现有 4.1.2 运行时上编译主代码和集成测试入口，不能写成“4.1 全部单测通过”。

## 复现

```bash
# 本机 Python + Spark 3.3/JVM 回归；构建实际引擎测试入口。
dev/test-transport.sh --spark=3.3

# 默认 RemoteService：真实 Spark + Ray，小规模且包含一次 task 重试。
dev/test-spark-ray-remote.sh

# 原生批次的大分区连续读取；两个快照共约 1.248 GB。
PYJAVA_INTEGRATION_ROWS=600000 PYJAVA_INTEGRATION_WIDTH=1024 \
  PYJAVA_INTEGRATION_PASSES=100 dev/test-spark-ray-remote.sh

# 兼容行接口。
PYJAVA_INTEGRATION_MODE=rows dev/test-spark-ray-remote.sh

# Maven 可用时的完整 Spark 4.1 suite。
dev/test-transport.sh --spark=4.1

# Maven 不可用时，现有远程 Spark 4.1.2 运行时的编译 + 实际引擎检查。
dev/test-spark41-runtime.sh
```

远程脚本默认使用 SSH 别名 `remoteservice`，可通过 `PYJAVA_LOADTEST_HOST` 更换；依赖该机器上现有的 Spark/JDK 路径和 Python Ray/PyArrow 环境。每轮只写自己的随机工作目录，结果下载到本机 `target/engine-validation/`。两个 Scala 版本使用独立 Maven 输出目录，避免混用二进制。

## 尚未覆盖的验收边界

当前证据支持单机实际 Spark/Ray 通信、有限期任务重读、受限批次内存以及本轮观察窗口内的资源回收。还没有验证多机网络故障、Ray actor 或 Spark executor 退出后的恢复、大量同时待消费分区的端口/磁盘压力、数小时运行以及 Flink connector/checkpoint。因此不能把这些结果称为完整跨引擎生产容量验收。

主机级流服务、共享监听入口、全局磁盘/连接配额、持久分区目录和 Flink checkpoint 接入仍是后续架构工作。现有补丁保留一次一条 Arrow 流的协议，不把这些能力隐藏在“Socket 池”名义下宣称已完成。
