# Python worker 读超时和数据 socket 读超时

协调进程的 socket 和分区数据 socket 不再共用 `python.socket.read.timeout`。

引擎 `Ray.runCoordinator` 把同一份 conf 交给 `ArrowPythonRunner`，也写进 Python context。`python.socket.transport=shared` 时，协调进程要等这一代表全部生成完才 `build_result`。生成期间 worker socket 上没有字节。若这里仍用数据面的读超时（例如 3000 毫秒），模型睡 8 秒、而每一代 `python.socket.shared.prepare.timeout.ms` 还有 30000 毫秒时，worker 会先被 `SO_TIMEOUT` 掐掉。慢模型就被记成网络读超时。

数据面的 3 秒不动。Python 收到的 `python.socket.read.timeout` 仍是原来的值。每一代的 prepare 期限仍只约束那一代（调度、模型、落盘），不拿来当整表截止时间，也不抄到 worker socket 上。

## 键

| 键 | 谁用 | 缺省 |
| --- | --- | --- |
| `python.socket.read.timeout` | 分区数据 socket 的读空闲超时。legacy worker 在没有下面那个键时，也继续用它做 `SO_TIMEOUT` | worker 路径缺省 `0`；数据 socket 路径各自的缺省不变 |
| `python.socket.worker.read.timeout` | 只决定协调 worker socket 的 `SO_TIMEOUT` | 不设置 |

`python.socket.worker.read.timeout` 以 `python.socket.` 开头，所以引擎现有的 ET 透传（`python.socket.`、`python.arrow.`、`python.connect.`）会原样送进 runner conf，不必改引擎。

## 取值

显式写了 `python.socket.worker.read.timeout` 时，shared 和 legacy 都用它，盖过另一条规则。

没写这个键时：

- `python.socket.transport=shared`：worker `SO_TIMEOUT` 为 `0`。生成期间没有输出不会被这个 socket 掐掉。卡住的那一代仍由协调进程里的 `python.socket.shared.prepare.timeout.ms` 放弃。协调进程要等所有代结束才往这个 socket 写结果，所以不能把“一代的 prepare”设成这个 socket 的超时，否则多代的表会在后面的代还没开始时被整表判死。
- 其它情况（没写 transport，或 `legacy`）：维持原语义，`SO_TIMEOUT` 等于 `python.socket.read.timeout`，缺省 `0`。

`0` 表示不设置超时（Java `Socket.setSoTimeout(0)`），不是“改回另一条默认”。这是 worker 原来的约定。负值直接拒绝，factory 不会把这次失败的配置留在池里。非整数字符串仍是解析错误。

数据 socket、握手、启动、连接、任务取消的超时都不看 `python.socket.worker.read.timeout`。

## 池

下面三项都是 factory 的池键。socket 在创建时写好 `SO_TIMEOUT`，借出去之前不会按后来的任务改：

- `python.socket.read.timeout`
- `python.socket.worker.read.timeout`
- `python.socket.transport`

因此 legacy 的 3000 毫秒 socket 不会借给 shared 的 0 毫秒池，两个显式 worker 超时也不会共用一条连接。`python.socket.shared.prepare.timeout.ms` 不是池键。只改 prepare、不改上面三项时，仍还回同一个 worker，socket 超时也不变。

时区这类任务参数继续不拆池。

## 不改的部分

JVM 和 Python 之间的帧格式、`ArrowSnapshotService`、Python 产品模块都不因这个超时拆分而改。分区数据读超时该是 3 秒就还是 3 秒。
