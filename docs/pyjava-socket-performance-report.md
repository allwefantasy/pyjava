# PyJava 传输稳定性与 Socket 控制报告

日期：2026-09-23

补充：Spark 分区与 Ray 之间的一次性 Arrow 通道已不再使用 common-utils。`SparkSocketRunner` 自己监听，批次直接写进 socket，不再先变成 `Array[Byte]`。PyJava 的 `common-utils` 依赖已经去掉。下面的正文仍是当时那次传输稳定性改动的记录。

这次改的是 PyJava 里 Java 和 Python 之间那条已有的 Socket 通道，不是另起一套 RPC。数据仍然用 Arrow 流过去。一个空闲连接对应一个 Python 进程加一条 TCP；同一时刻只服务一个任务。两条互不相关的任务不会共用一条正在传数据的连接。

本地路径：`/Users/williammacintel/projects/pyjava`。实现说明还在同目录的 `docs/transport-reliability.md`。

## 原先的问题

复用是为了少启动 Python 进程。进程启动、解释器初始化和第一次 Arrow 握手都比传一小批数据贵。但原来的复用把稳定性押在「上次任务正常结束，这条连接就还能用」上。对端可以在探测之后立刻退出，半关的连接、残留的协议字节、重复归还，都会让下一次任务读到不该读的东西，或者把同一条连接租给两个任务。

另一头是内存。JVM 侧以前可能把整个分区塞进一个 Arrow batch。行数一多，写入线程和 Python 端会同时扛住这一批。

## Socket 归谁管

`PythonWorkerFactory` 持有 Python 进程和它们的 Socket。`IdleWorkerPool` 只保存空闲连接。池上的方法都在所属 factory 的锁里跑，避免借出和归还交错。

进程级的注册表锁不再包住阻塞的进程启动和 Socket 读写。一个环境启动慢，不会卡住另一个池的借还。池的配置参与 factory 的身份：解释器、环境变量，以及 daemon 模块、空闲上限、空闲时间、连接超时、启动超时、读超时、探测超时这些项不同，就会落到不同的池。只影响单次任务、不影响池身份的选项，仍然共用同一个 factory。

归还时按这条 Socket 的真正主人送回。`owners` 用 `WeakHashMap` 记下 Socket 属于哪个 factory，避免调用方拿错环境去释放。

## 空闲池怎么控

`IdleWorkerPool` 用 `LinkedHashMap` 按进入顺序保存空闲 Socket，并记下放入时间。

借出时先清掉过期连接，再从最老的一条开始拿。每条都先做健康检查。检查不过就关掉，继续下一条。池空了才新建 worker。

归还有三条规则：

- 同一条 Socket 已经在池里，第二次归还直接忽略。这样不会把一条连接同时租给两个任务。
- 池里空闲条数已经达到上限，或者本机看来连接已经关上，就丢掉，不进池。
- 否则记下当前时间，留着下次用。

默认每个环境、每套配置最多留 64 条空闲连接，对应 `python.worker.pool.maxIdle`。设成 0 就是不再保留空闲 worker。空闲寿命默认 1 分钟，对应 `python.worker.idle.time`，单位是分钟，必须大于 0。后台还有一条 daemon 线程，大约每 10 秒扫一次过期连接，避免池子只在下一次借出时才清理。

丢掉空闲 worker 时只关 Socket，并销毁非 daemon 模式下对应的进程。不会拿缓存的 PID 去通知 daemon 杀进程。空闲进程在等下一次请求，对端收到 EOF 就会退出。死进程的 PID 可能已经被系统复用，这时再发杀死信号会打到别人头上。

## 复用前的探测

健康检查不发新的协议消息。空闲 worker 本来就不该主动写字节。如果为了探测去发 ping，旧 worker 会把这段字节当成下一次任务的协议，连接就废了。

做法是把读超时临时改成 `python.worker.validate.timeout`，默认 1 毫秒，然后读一个字节：

- 读超时，说明对端安静，连接可以复用。然后把原来的读超时设回去。
- 读到字节、读到 EOF，或者连接重置，说明上面有残留数据或连接已经坏了，丢掉。
- 本机已经关闭、输入或输出已经 shutdown，也不复用。

探测成功之后，对端仍然可能马上死掉。这种失败会结束当前任务。代码不会在失败后自动把同一段 Python 再跑一遍，因为用户代码可能有副作用。TCP keepalive 只是操作系统的机制，不是应用层心跳。

## 建连、超时和 daemon

Unix 上默认走 `pyjava.daemon`，由它 fork worker。Windows 强制走独立 worker，不走 daemon。`python.use.daemon` 可以关掉 daemon。

新建连接时打开 `TCP_NODELAY` 和 keepalive。`TCP_NODELAY` 避免小包被 Nagle 算法攒住，对这种请求加 Arrow 流的交互更重要。Python 侧 `daemon.py` 和 `utils.py` 同样设置了 `TCP_NODELAY`。

几段等待是分开计时的：

| 配置 | 默认 | 含义 |
| --- | --- | --- |
| `python.connect.timeout` | 10000 毫秒 | 连上 daemon 的 TCP 期限 |
| `python.worker.startup.timeout` | 10000 毫秒 | daemon 报端口、PID 握手，以及独立 worker 回连的期限 |
| `python.socket.read.timeout` | 0 | 任务进行中的读超时。0 表示不主动掐掉长任务，它不是整个任务的截止时间 |
| `python.worker.validate.timeout` | 1 毫秒 | 空闲探测的读超时 |
| `python.worker.pool.maxIdle` | 64 | 每个池最多留多少空闲连接。0 表示不保留 |
| `python.worker.idle.time` | 1 分钟 | 空闲连接能活多久 |
| `python.task.killTimeout` | 20000 毫秒 | 任务被打断后，多久再强制取消 |
| `python.arrow.maxRecordsPerBatch` | 10000 | JVM 写出时每批最多多少行 |
| `py_worker_reuse` | true | 正常结束后是否把连接放回池里 |
| `buffer_size` | 65536 | 缓冲字节数，原有配置 |

daemon 启动时先把 stderr 引走，再读它宣布的端口，避免 import 把管道写满、握手读不到端口。端口必须落在 1 到 65535。握手失败如果是因为 daemon 进程已经退出，会重启 daemon 再试一次获取。这次重试发生在用户代码和数据发出去之前。daemon 还活着、只是某一个 worker 没起来时，不会把整个 daemon 杀掉，免得牵连别的任务。fork 失败返回负数错误码。

独立 worker 模式下，Java 在本机 `127.0.0.1` 上听一个临时端口，把端口放进 `PYTHON_WORKER_FACTORY_PORT`，等 Python 回连。启动失败会关 Socket，并强制销毁已经拉起的进程。

任务进行中的读超时在借出后按配置设上。探测时改过的超时，在探测结束时恢复，不会把 1 毫秒的探测超时留到正式任务上。

## 任务结束时连接去哪

正常结束要看到 Python 的结束标记。结束标记之后如果输入流里还有字节，直接当协议错误，这条连接不再回池。`releasedOrClosed` 保证释放或销毁只发生一次。

`py_worker_reuse` 为真，并且 daemon 还活着，连接才 `release` 进空闲池。否则销毁。异常、取消、提前结束，走销毁，不回池。

取消正在跑的 daemon worker 时，才会把 PID 写给 daemon，让它处理这个子进程。这和丢弃空闲连接不同：这时任务还占着这条连接，PID 对应的是当前子进程。

factory 停止时先停监控线程，清掉空闲池，关上 daemon 名下的 Socket，再强制结束独立 worker 进程。停掉的 factory 再 `create()` 会直接拒绝。

`SparkSocketRunner` 那条读路径是一次性的。它也设置了连接和读的期限，成功、出错或任务提前结束都会放开 Socket。它的服务端协议不支持在同一条连接上接第二轮请求，所以不进这个空闲池。

## Arrow 不再整分区进一批

`ArrowPythonRunner` 读 `python.arrow.maxRecordsPerBatch`，默认 10000，必须是正数。写出线程每写满这么多行就 `finish`、`writeBatch`、`reset`，再写下一批。这是行数上限，不是字节上限。单行特别大时，这一批仍然可能很占内存。

测试里用 25001 行、每批 1024 行，确认数据会拆成 25 批走完，而不是一整块。写入线程自己持有 Arrow 的 `VectorSchemaRoot` 和 allocator，在 finally 里关闭，避免和任务结束回调抢着关同一块内存。

## Python 侧把请求边界补齐

如果用户的 Python 代码不读输入，或者只读了一截，worker 会在确认完成之前把剩下的 Arrow 输入排空。否则下一次复用会从半截流开始读。意外的结束标记会让这次任务失败，而不是把一条可能已经乱掉的连接放回池里。

独立连接在包上缓冲流之前会先设好超时。daemon 侧复制出来的描述符在每次请求后显式关掉。取消只针对还没有回收的子进程 PID。`BUFFER_SIZE` 按配置生效，旧的 `SPARK_BUFFER_SIZE` 仍可作为后备。

## 测过的和没测的

2026-09-23 在本机做过一轮 Spark 3.3 验证：macOS、JDK 17、Spark 3.3.0、Scala 2.12.15、Arrow Java 7.0.0、Python 3.9.6、PyArrow 18.1.0、pandas 2.2.3。结果是 9 个 Python 测试和 36 个 JVM 测试通过，没有失败、取消或跳过。日志在 `target/transport-validation/spark33.log`。

覆盖的情况包括：健康连接复用且不会把同一次归还租出两次；不同池的配置互相隔离；关掉的 Socket 会被换掉；一个池的启动卡住时，另一个池仍能归还和借出；工厂关闭后拒绝新的借出；空闲容量、过期、重复归还；启动和握手的期限；远程 EOF、重置和空闲数据损坏；未读完或部分读取的输入；worker 崩溃；取消和读超时；独立进程模式；多批往返之后 allocator 能收回。

可选的基准是 1000 行送进 Python 再返回 1 行，5 次预热、30 次计时。同一次修改里对比「开复用」和「关复用」，不是和改之前的旧版本比。几次跑出来的中位数差得很远，例如一轮复用开/关大约是 14.0 / 24.8 毫秒，另一轮是 61.1 / 67.8 毫秒。机器上还有别的负载，Python 垃圾回收和批量大小都会动这些数，不能当成稳定加速比。

Spark 4.1 / Scala 2.13 这条构建当时没有验完。默认依赖 `tech.mlsql:common-utils_2.13:1.0.0`，Maven Central 上没有这个包。common-utils 源码后来已经能同时编 2.12 和 2.13，本地路径是 `~/projects/common-utils`，用 `-Pscala-2.13` 得到 `common-utils_2.13:1.0.0`。那次改动还没有装进 PyJava 再跑 Spark 4.1 测试。Windows、多 Executor 集群和 Ray service 也没有跑。

## 使用时可以先动的旋钮

连接数不想留那么多，把 `python.worker.pool.maxIdle` 调小。任务很长、中间可能几十秒没有字节回来，不要把 `python.socket.read.timeout` 设成一个很短的数，那个值是读超时，不是空闲心跳。只有确认 Python 代码可以安全重跑时，才考虑在任务失败后由调用方自己重试；这条通道不会自动重放。
