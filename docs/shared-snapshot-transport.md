# 共享分区快照

协议名 `pyjava-arrow-snapshot/1`。只有配置里显式写了 `python.socket.transport=shared` 才走这条路径。不写这个键时，`serveToStreamWithArrow`、`readFromStreamWithArrow` 和三列 `host,port,server_id` 结果保持原样。`python.socket.detached=true` 仍是另一条一次性文件端点，不是本协议。

下面凡是名字以 `.ms` 结尾、或本来就写明 timeout 的配置，单位都是毫秒。字节配额是字节。小于等于 0 会直接拒绝。

## 引擎要调用的方法

`SparkSocketRunner`：

- `exportToStreamWithArrow(iter, schema, maxRecordsPerBatch, context, conf): PartitionDescriptor`
- `readFromSharedSnapshot(host, port, token, context, conf): Iterator[InternalRow]`
- `releaseSharedSnapshot(host, port, token, conf): Boolean`
- `sharedSnapshotStatus(host, port, token, conf): (Int, String)`

`PartitionDescriptor` 的行顺序是 `host, port, token, protocol, partition_id, attempt_id, snapshot_bytes, lease_deadline_ms, timezone`。`protocol` 固定为 `pyjava-arrow-snapshot/1`。`partition_id` 和 `attempt_id` 可以是 0，0 不是“缺失”。

Python 侧 `RayContext.setup` / `map_batches` 在 shared 模式下返回同一组字段。输入行如果带了 `protocol` 或 `token`，或者当前就是 shared 模式，却不是这个协议，会报错，不会退回旧的一次性 socket。

## 线格式

请求：`int32 magic(0x50594A53) | int32 version(1) | int32 op | int32 tokenLen | token UTF-8`。

op：`1=STATUS`，`2=READ`，`3=RELEASE`。

响应：`int32 status | int32 infoLen | UTF-8 JSON`。status：`0=READY`，`1=PREPARING`，`2=FAILED`，`3=EXPIRED`，`4=UNKNOWN_TOKEN`，`5=BUSY`，`6=BAD_REQUEST`。

READY 的 READ 后面再跟 `int64 length | 快照字节`。字节是旧版 Arrow IPC。同一条 TCP 可以按顺序请求不同 token。调用方必须把上一次 READ 读完才能发下一次。帧坏了这条连接就不能再复用。

`fetch_shared_arrow_batches` 每次新建一条连接，用完即关，包括生成器提前结束。协议允许调用方在自己持有的一条连接上按顺序请求多个 token，但这不是自动 TCP 连接池，客户端也不会把读请求复用到一个隐藏的池里。

## 谁活着，谁退出

Spark 源分区：`exportToStreamWithArrow` 在生产 task 里把分区写成文件并注册 token。task 成功结束不会关掉这份快照，之后在 `lease_deadline_ms` 之前可以再读，断线也可以重读。读完不会自动 `release`。引擎在确认不再需要时调用 `releaseSharedSnapshot`。

生产 task 失败时，即使文件已经写完，这份 attempt 也会标成失败并删文件。Spark 重试是一次新的 `taskAttemptId`，因此是新 token，不会接着用失败那次的文件。源快照的租期从提交成功开始算，不是从后来的读取开始算。想在失败或取消之后还能读，靠的是这段租期，不是无限保留。

Python/Ray 结果：一次 `RayContext.setup` 创建有界个 detached `RaySnapshotWorker`。数量默认是 `min(分区数, python.ray.inflight.generations)`，也可以用 `python.ray.snapshot.actors` 单独指定，但同时运行的生成数不会超过 `python.ray.inflight.generations`（默认 2）。每个 actor 进程里只有一个 `SnapshotService`，上面挂多个 token。不会每个分区新开一个 actor。

生成名额在物化返回时释放，不等 Spark 来读。多出来的分区不会先排进 actor 的方法队列。名额不够、注册表满或字节预算不够，会立刻失败，不会等以后的消费者。模型回调可以自己睡，也可以抛错。

每个已经提交的 generation 单独使用 `python.socket.shared.prepare.timeout.ms`。时钟从这次 `generate` 提交开始，包含三段：Ray 把方法调度到 actor、模型 transform、以及物化把生成器写成文件。前面的分区花掉的时间不会从后面还没提交的分区里扣，整表也不是只给一次期限。激活和之后的租期不在这三段里面。

回调一直不返回，或者生成器一直不产出下一批时，服务把 token 标成失败并不会让卡在原生调用里的那条线程退出来。CPython 不能安全地抢占它。actor 里另有一条 watchdog，到期限后关掉监听、删掉这个 actor 独占的目录，再结束进程。协调器不会把 `release` / `shutdown` 排进被 `generate` 占住的方法队列里干等。actor 的 `max_concurrency` 是 2，多出来的名额只给 `abandon` 这种清理，不给第二个 generation。`abandon` 先删目录再退出进程；短等之后进程还在，才 `ray.kill(..., no_restart=True)`。`ray.kill` 不跑 atexit，所以不能倒过来先杀进程再指望清理函数删文件。只清理这次 setup 创建的快照 actor，不调用 `ray.shutdown`，也不结束外部模型 actor。清理过程里的新异常不会换掉原来的模型异常。激活失败，或者协调进程上的 KeyboardInterrupt，走同一条回收。

同一轮里先写完的快照先不开始租期。协调还在跑时会续上“尚未激活”的期限，全部生成结束后一起激活，返回给引擎的 `lease_deadline_ms` 才是真正的截止时间。这样前面的分区不会在后面的分区还在写时过期。

`generate` 正常返回后 actor 不退出。激活之后，快照还在原来的 host/port/token 上，可以再读，直到这次租期结束。协调器成功返回时会调用 `enable_idle`。此后如果所有 token 都没了，再空闲 `python.ray.snapshot.actor.idle.ms`（默认 60000），actor 关掉监听、删掉自己的目录，再结束进程。

协调进程如果被 Spark 取消，进程会直接消失，Python 的 `finally` 不会跑，`enable_idle` 也不会被调用。这种情况下 actor 自己还有一条有限期：`python.ray.snapshot.actor.orphan.ms`（默认 60000）。从构造、`keepalive` 或上一次 `generate` 返回起算，这段时间里没有新的协调心跳，并且当前没有一张已经激活、租期还没到的快照，actor 就自己退出。这覆盖两类：刚构造、还没收到 `generate` 的 actor；`generate` 已经返回、但还没 `activate` 的 actor。退出仍然是 actor 进程里的 `service.close`（监听关掉，文件 unlink，写线程握着 fd 也删目录项），再删掉这个 actor 的目录。协调器在本机上删 workdir 不能代替这一步。不会调用 `ray.shutdown`，也不会停掉用户的 Ray 集群，旁边的模型 actor 不受影响。已经激活的快照不走这条提前退出，仍然可读到租期结束。

prepare 超时是另一条退出路径，只发生在这次 generation 没有在期限内返回的时候。

## 配额

同一个进程里，`(python.socket.shared.name, host, python.socket.shared.port)` 第一次打开监听时定下三个进程级预算：

| 键 | 默认 |
| --- | --- |
| `python.socket.shared.total.maxBytes` | 4294967296 |
| `python.socket.shared.maxSnapshots` | 256 |
| `python.socket.shared.maxConnections` | 64 |

后来的请求如果给了不同的值，直接拒绝，不会悄悄沿用第一次的值却让调用方以为新值生效了。不写这些键表示接受已经打开的服务。换一个 `python.socket.shared.name` 就是另一个服务。

租期、准备期限和单分区字节上限是每次注册自己的，可以和进程预算不一样：

| 键 | 默认 | 含义 |
| --- | --- | --- |
| `python.socket.shared.lease.ms` | 1800000 | 可读窗口。源快照从提交成功起算；Ray 结果从整批激活起算 |
| `python.socket.shared.prepare.timeout.ms` | 同租期 | 每个已提交 generation 的上限，含调度、transform、物化。不是整表一次，也不含激活和租期 |
| `python.socket.shared.activate.timeout.ms` | 同租期 | Ray 结果已写完但还没激活的上限。协调过程中会续期 |
| `python.socket.shared.partition.maxBytes` | `python.socket.spool.maxBytes`，否则 1073741824 | 单个快照 |
| `python.socket.shared.expired.grace.ms` | 60000 | 过期后描述符还留多久 |
| `python.socket.shared.wait.ready.ms` | 1800000 | 读的一边等多久 |
| `python.socket.shared.status.poll.ms` | 100 | STATUS 轮询间隔 |
| `python.socket.shared.handshake.timeout` | 10000 | 单次握手的 socket 超时 |
| `python.ray.inflight.generations` | 2 | 这次 setup 同时物化的分区数 |
| `python.ray.snapshot.actors` | 同 inflight | 这次 setup 的结果 actor 数 |
| `python.ray.snapshot.actor.idle.ms` | 60000 | `enable_idle` 之后，token 清空再等多久退出 actor |
| `python.ray.snapshot.actor.orphan.ms` | 60000 | 没调用 `enable_idle`、也没有正在生效的租期时，多久没有协调心跳就退出。包括从未 `generate`，以及 `generate` 完成但未激活 |
| `python.ray.snapshot.group` | 随机 | 只用于给这批 actor 起名 |

字节预算记的是还开着的文件。`release`、租期结束或失败会马上拒绝新的读取，但只要还有读者把文件打开着，预算就不还。最后一个读者关掉并且文件删掉，才还这一次。还账只做一次，不会减成负数。preparing 时被 release、超时或写失败，写的一边会在下一次检查时停。检查发生在批次之间；生成器如果不产出，这条检查不会跑到。

快照文件不直接放在调用方给的目录里。每个 `SnapshotService` 在 `python.socket.shared.dir`（或 `PYJAVA_SPOOL_DIR`，否则系统临时目录）下面建一个自己的子目录。Ray actor 再往上包一层只属于这个 actor 的目录，清理时只删这一层，不删父目录里的其它文件。写线程还握着文件描述符时，POSIX 上先 unlink：目录项马上消失，inode 要等进程退出、描述符关掉才释放。所以超时清理是“拥有者先删目录，再结束进程”，不是只杀进程把文件留在共享临时目录里。持有 GIL 的纯计算循环会挡住同一进程里的 watchdog；这种情况下协调器仍会 `ray.kill`，本机上的协调进程也能按 actor 目录删掉文件。远程 worker 上如果杀进程抢在删目录之前，那个 actor 目录可能留下来，位置就是上面的独占子目录。

服务关掉时会关掉已经接受的连接，并删掉自己的子目录，而不是只关监听再干等读超时。

## 不支持

- 不自动池化快照连接。
- 不在每次读完后自动 release。
- 不把失败 attempt 的文件留给下一次 Spark 重试。
- 不覆盖标量 UDF、HTTP 客户端，也不改 Byzer-LLM 的 actor 协议。Byzer 当前没有 `map_batches` 调用方；shared 只包住 `RayContext.setup` 这条转换。
- 不调用 `ray.stop` / `ray.shutdown`。
