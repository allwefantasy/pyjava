# Socket and Arrow transport reliability

This change hardens Python-worker reuse and Spark/Ray partition exchange. A pooled
entry is a Python process plus its TCP connection; each entry serves one task at
a time. Independent tasks never share a live Arrow stream.

The latest partition lifecycle, native Arrow batch API and real-engine results
are in [the Spark/Ray validation report](spark-ray-transfer-validation-2026-09-23.md).
Worker pooling and cross-engine partition serving have separate lifecycles.

## What changed

- **Bounded idle retention.** Each environment/configuration pool retains at most
  64 idle workers by default. Expiration is measured separately for each socket.
  Duplicate returns cannot lease the same socket to two tasks. Active task
  concurrency is still controlled by the application or Spark; this is not a
  global limit on active workers.
- **Validation before reuse.** A short read probe rejects EOF, reset,
  half-closed connections and unexpected pending data. An idle worker has no
  valid response to send. The probe uses no new wire messages, and restores the
  task's read timeout afterward.
- **Bounded acquisition.** TCP connect, daemon port discovery, PID handshake and
  standalone-worker callback have deadlines. Failed acquisition closes its
  socket. A healthy daemon is not restarted because one child failed to start.
  Fork failures return a negative error code without killing other workers.
- **Independent pools.** The process-wide registry lock no longer covers
  blocking process startup or socket I/O. Pool configuration participates in
  identity; releasing/destroying a socket routes through its actual owner.
- **Lifecycle cleanup.** Failure, cancellation, protocol errors, early result
  termination and normal completion release sockets and Arrow allocations.
  The task monitor stops once its lease is returned; it cannot subsequently
  kill that socket after another task borrows it. Factory shutdown terminates
  its monitor, closes all sockets and stops its processes.
- **Python transport cleanup.** Standalone connection timeouts are reset before
  creating buffered file streams. Duplicated daemon descriptors are explicitly
  closed after every request. Cancellation targets only unreaped child PIDs.
  The configured BUFFER_SIZE is honored, with the older SPARK_BUFFER_SIZE
  accepted as a fallback.
- **Complete request boundaries.** If Python user code ignores input or reads
  only a prefix, the worker drains the remaining Arrow input before acknowledging
  completion. Unexpected end markers now fail the task instead of allowing a
  potentially corrupt connection back into the pool.
- **Bounded Arrow batches.** JVM input is split at 10,000 records per batch by
  default instead of accumulating an entire input partition in one Arrow
  batch. This is a row-count bound, not a byte limit; very large individual
  values can still consume significant memory.
- **Barrier callback.** Only a task whose context reports `isBarrier` binds one
  extra `127.0.0.1` socket (backlog 8) for the life of that task. Python
  `context.barrier()` and `BarrierTaskContext.get().barrier()` connect, write
  function id `1`, and wait for one UTF-8 string: `success`, or the JVM
  exception text. This is not PySpark's authenticated multi-message framing.
  `allGather()` and `getTaskInfos()` are rejected. A normal task sends port 0
  and does not listen. The callback socket is closed when the task ends and is
  never entered into the idle pool. A failed callback fails that task; the
  worker is not reused.
- **Partition streams.** SparkSocketRunner's read path has connect/read
  settings, observes task cancellation, and releases the socket on success,
  error or early task completion. Each TCP connection carries one stream.
  The serve path no longer goes through common-utils.
  `ArrowSocketServer` listens with a backlog of 8, sets `TCP_NODELAY`, keepalive
  and a 1 MB socket buffer, and `ArrowConverters.writeLegacyArrowStream` writes
  legacy Arrow IPC straight to that buffer. The bytes match the previous
  `ArrowBatchStreamWriter` stream, including the 4-byte end marker rather than
  an Arrow continuation token. Python still sets `ARROW_PRE_0_15_IPC_FORMAT`.
  If the reader never connects or the write fails, `ServerSocket.isClosed`
  throws after the socket is closed, so a task waiting on that flag cannot
  treat a dropped partition as success. Direct streaming ends with its task;
  `python.socket.detached=true` first commits a quota-limited snapshot on the
  owning task, then keeps its one-shot endpoint alive until consumption or the
  accept deadline. RayDataServer also commits a bounded snapshot, and accepts
  new readers until its replay lease expires. A retry reads this same snapshot
  without rerunning user code. Arrow buffers are released by their owner after
  socket I/O stops. Python readers require the IPC end marker to reject silent
  truncation. OnceServer bounds batches by rows and bytes and validates ACKs.
- **Socket latency.** Both worker endpoints enable TCP_NODELAY and keepalive.
  An established task is never automatically replayed after a failure.

A peer can still die immediately after a successful idle validation. Such a
failure ends the task; blindly retrying Python code could repeat its side
effects. Keepalive is an OS mechanism, not an application heartbeat.

## Configuration

Pass these entries in the ArrowPythonRunner configuration map:

| Key | Default | Meaning |
| --- | --- | --- |
| py_worker_reuse | true | Reuse workers after a valid end-of-stream handshake. |
| python.worker.pool.maxIdle | 64 | Maximum idle workers per pool; 0 disables retention. |
| python.worker.idle.time | 1 | Positive idle lifetime in minutes. |
| python.worker.validate.timeout | 1 | Positive idle probe timeout in milliseconds. |
| python.connect.timeout | 10000 | Positive TCP connection deadline in milliseconds. |
| python.worker.startup.timeout | 10000 | Positive daemon startup/PID/worker callback deadline in milliseconds. |
| python.socket.read.timeout | 0 for workers; 300000 for SparkSocketRunner | Socket read timeout in milliseconds; 0 disables it. This is not an overall task deadline. |
| python.use.daemon | true | Use the Unix forking daemon. Windows always uses standalone workers. |
| python.arrow.maxRecordsPerBatch | 10000 | Positive JVM input batch row limit. |
| buffer_size | 65536 for workers; 1048576 when unset on a Spark/Ray data socket | Buffered transport bytes. `python.socket.buffer` overrides the data socket. |
| python.socket.buffer | 1048576 | Send/receive and stream buffer for SparkSocketRunner, in bytes. 1..67108864. |
| python.socket.accept.timeout | 300000 | How long a served partition waits for its one reader, in milliseconds. |
| python.socket.write.timeout | 300000 | Spark partition writer no-progress timeout in milliseconds; 0 disables it. Cancellation still closes the socket. |
| python.socket.detached | false | Commit a temporary Arrow snapshot before returning an endpoint from the producing Spark task. |
| python.socket.spool.maxBytes | 1073741824 | Per-partition temporary-file quota for detached Spark exports. |
| python.arrow.maxBytesPerBatch | 8388608 | Arrow vector data byte limit per SparkSocketRunner output batch; IPC metadata is additional. |
| python.arrow.maxAllocation | 134217728 | Per-reader/writer Arrow allocator limit for SparkSocketRunner, in bytes. |
| PYJAVA_ARROW_MAX_RECORDS_PER_BATCH | 8192 | Rows per Arrow batch when Ray's OnceServer writes back to Spark. |
| PYJAVA_ARROW_MAX_BYTES_PER_BATCH | 8388608 | Python output batch data byte limit; oversized individual rows fail. |
| PYJAVA_SPOOL_MAX_BYTES | 1073741824 | Per-partition Ray output snapshot quota, including IPC bytes. |
| PYJAVA_SPOOL_DIR | system temp directory | Directory for Python snapshot files. |
| PYJAVA_REPLAY_TTL_SECONDS | 300 | Ray snapshot admission lease, starting after materialization completes. |
| PYJAVA_SOCKET_TIMEOUT_SECONDS | 300 | Python data-socket I/O timeout. |
| PYJAVA_ACK_TIMEOUT_SECONDS | 30 | Python server acknowledgement deadline. |
| PYJAVA_ACCEPT_TIMEOUT_SECONDS | 300 | One-shot Python server accept deadline. |
| python.task.killTimeout | 20000 | Delay before forcibly cancelling an interrupted task, in milliseconds. |

For example:

```scala
val transportConf = Map(
  "py_worker_reuse" -> "true",
  "python.worker.pool.maxIdle" -> "16",
  "python.worker.idle.time" -> "2",
  "python.connect.timeout" -> "10000",
  "python.worker.startup.timeout" -> "30000",
  "python.socket.read.timeout" -> "0",
  "python.arrow.maxRecordsPerBatch" -> "10000"
)
// Supply transportConf to ArrowPythonRunner's existing conf argument.
```

The existing three-argument SparkSocketRunner.readFromStreamWithArrow method is
preserved. Its new four-argument overload accepts a configuration map for
python.connect.timeout and python.socket.read.timeout.

At application shutdown, after tasks finish or are cancelled, call
PythonWorkerFactory.shutdownAll(). Close each JavaContext in a finally block;
close now marks the context complete, runs all completion listeners even if one
fails, and is safe to repeat.

If integrating a custom CommonTaskContext, its readerRegister implementation
must invoke the supplied cleanup callback and close only non-null reader and
allocator arguments. The bundled Java and Spark implementations do this. Lazy
Arrow readers are captured by the callback so early termination closes the
actual reader rather than its original null value.

## Validation

Run the reproducible transport suite from the repository root:

```bash
# Creates an isolated target/transport-venv when no interpreter is supplied.
# Use Python 3.9 or 3.10 for the repository's bundled legacy cloudpickle.
dev/test-transport.sh --spark=3.3

# Use an existing environment and include the opt-in latency samples:
PYJAVA_TEST_PYTHON=/path/to/python PYJAVA_BENCHMARK=1 \
  dev/test-transport.sh --spark=3.3

# Normal Maven arguments can follow the selector, e.g. -s /path/to/settings.xml.
# The repository's default dependency line can be checked separately:
dev/test-transport.sh --spark=4.1
```

The suite uses real TCP sockets, Python subprocesses and Arrow IPC. It covers
remote EOF/reset, corrupted idle data, duplicate returns, idle capacity and
expiry, startup/handshake deadlines, isolated pool locking, repeated requests,
parallel callers, unused/partial input, worker crashes, input-writer exceptions,
cancellation, read timeouts, standalone mode, complete multi-batch round trips,
one-shot stream cleanup and allocator reclamation. It also runs the existing
Arrow conversion, row encoding and local Spark DataFrame tests.

Validation on 2026-09-23 used macOS Intel, JDK 17, Spark 3.3.0, Scala 2.12.15,
Arrow Java 7.0.0, Python 3.9.6, PyArrow 18.1.0 and pandas 2.2.3. The test script
sets the JDK module opens needed by this Spark/Arrow runtime. This is local
library validation, not deployment or acceptance of a running InfiniSQL service.

The latest run passed **28 Python tests and 50 JVM tests**. One optional JVM
latency benchmark was disabled. Evidence is in
`target/transport-validation/final-python.log` and `final-spark33.log`.
PyJava no longer depends on tech.mlsql:common-utils.

Separate isolated runs on RemoteService exercised real Spark 3.3 and 4.1.2
processes with Ray 2.47.1, including task retry and repeated partition reads.
The Spark 4.1 run compiled main sources against the installed runtime; it is
not the full Maven test suite, which was blocked by artifact download timeouts.
Windows, cross-machine networking, multi-executor clusters and Flink were not
validated. Detailed measurements and reproduction commands are in the linked
Spark/Ray report.

## Interpreting the benchmark

The optional benchmark sends 1,000 rows, consumes them in Python and returns one
row. Each configuration gets five warmup calls and 30 measured calls. One run
recorded median/p95 times of 14.013/14.807 ms with reuse and 24.819/32.563 ms with
reuse disabled. Another run on the same shared machine recorded
55.880/69.145 ms and 62.565/69.169 ms respectively.
The final run measured 61.130/71.752 ms with reuse and 67.824/80.166 ms without it.

These compare reuse enabled with reuse disabled in the modified code. They are
not an old-version/new-version speedup measurement. Host load, Python garbage
collection, data shape and batch size affect the result. The deterministic
memory check confirms that 25,001 rows at a 1,024-row limit cross the JVM input
stream in 25 batches and that early result close returns Arrow allocations to
their prior level.

## Rollout boundary

The edits are an unreleased working-tree change. Update both the JVM library and
the Python pyjava package, then restart the application's worker factories so
existing daemon processes do not keep old Python code loaded. The wire control
markers and Arrow IPC format are unchanged. Interactive state still belongs to
the worker process: an expired or failed worker loses that in-memory state.

The work does not change InfiniSQL's dependency declarations, publish a Maven or
PyPI release, or replace any running service.

The Python timeout behavior is documented in
[the Python socket.makefile reference](https://docs.python.org/3/library/socket.html#socket.socket.makefile).
