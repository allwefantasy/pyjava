# AGENTS.md instructions for pyjava

## Related Projects

- PyJava 不再依赖 `tech.mlsql:common-utils`。日志在 `tech.mlsql.arrow.log.Logging`。Spark 分区和 Python/Ray 之间的一次性 Arrow socket 在 `ArrowSocketServer` / `SparkSocketRunner`，不要再接回 `SocketServerInExecutor`。
- `PythonProjectRunner` 仍直接使用 `com.lihaoyi:os-lib`。默认 `os-lib.version` 是 `0.7.8`；`-Pscala-2.11` 用 `0.2.9`。
- SQL 大表进 Python/Ray 做摘要、分类、embedding 的组件边界见 `docs/sql-table-ray-model-components.md`。`MasterSlaveInSpark` 不在本仓库，而在 Infinity SQL：`infinity-sql/streamingpro-mlsql/src/main/java/tech/mlsql/tool/MasterSlaveInSpark.scala`。它只被 `Ray.distribute_execute` 调用，负责在 driver 收集各分区地址；分区上的 Arrow 服务由本仓库的 `SparkSocketRunner.serveToStreamWithArrow` 提供。当前调用没有传入 `python.socket.detached=true`。
