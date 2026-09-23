# AGENTS.md instructions for pyjava

## Related Projects

- PyJava 不再依赖 `tech.mlsql:common-utils`。日志在 `tech.mlsql.arrow.log.Logging`。Spark 分区和 Python/Ray 之间的一次性 Arrow socket 在 `ArrowSocketServer` / `SparkSocketRunner`，不要再接回 `SocketServerInExecutor`。
- `PythonProjectRunner` 仍直接使用 `com.lihaoyi:os-lib`。默认 `os-lib.version` 是 `0.7.8`；`-Pscala-2.11` 用 `0.2.9`。
