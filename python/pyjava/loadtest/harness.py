"""Self-contained load for the three PyJava sockets.

This does not start Spark or Ray, and it does not touch a running SQL engine.
It drives the same Python pieces those engines use:

- worker-reuse: one daemon, several concurrent workers, many Arrow requests
- barrier: those workers call context.barrier() against a callback server
  that speaks the JVM's one-string reply
- spark-ray: several OnceServer partitions (the Ray return path) read back
  the way a Spark task acknowledges a finished stream

The JVM accept loop itself is covered by ArrowTransportSpec, not by this process.
"""

import argparse
import concurrent.futures
import json
import os
import platform
import select
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time

import pyarrow as pa

from pyjava.api.serve import OnceServer
from pyjava.transfer import StrictArrowInput
from pyjava.serializers import SpecialLengths


def _read_int(stream):
    data = stream.read(4)
    if len(data) != 4:
        raise EOFError("short header")
    return struct.unpack("!i", data)[0]


def _write_int(stream, value):
    stream.write(struct.pack("!i", value))


def _write_text(stream, value):
    data = value.encode("utf-8")
    _write_int(stream, len(data))
    stream.write(data)


def _percentile(samples, percent):
    if not samples:
        return None
    ordered = sorted(samples)
    index = int(round((percent / 100.0) * (len(ordered) - 1)))
    return ordered[max(0, min(index, len(ordered) - 1))]


def _descriptor_count():
    for path in ("/proc/self/fd", "/dev/fd"):
        if os.path.isdir(path):
            return len(os.listdir(path))
    return None


class Daemon:
    def __init__(self):
        self.workspace = tempfile.TemporaryDirectory(prefix="pyjava-load-")
        self.stderr = tempfile.TemporaryFile(mode="w+b")
        env = dict(os.environ, PY_WORKER_REUSE="1", BUFFER_SIZE="65536")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "pyjava.daemon"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
            env=env, cwd=self.workspace.name)
        if not select.select([self.process.stdout], [], [], 30)[0]:
            self.close()
            raise RuntimeError("daemon did not announce a port")
        try:
            self.port = _read_int(self.process.stdout)
        except EOFError:
            self.stderr.seek(0)
            detail = self.stderr.read().decode("utf-8", "replace")
            self.close()
            raise RuntimeError(detail or "daemon exited before announcing a port")

    def connect(self):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=10)
        sock.settimeout(60)
        infile = sock.makefile("rb")
        outfile = sock.makefile("wb")
        pid = _read_int(infile)
        if pid <= 0:
            infile.close()
            outfile.close()
            sock.close()
            raise RuntimeError("daemon failed to fork (%s)" % pid)
        return _Worker(sock, infile, outfile, pid)

    def close(self):
        if self.process.poll() is None:
            try:
                self.process.stdin.close()
            except Exception:
                pass
            try:
                self.process.wait(10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(5)
        try:
            self.process.stdout.close()
        except Exception:
            pass
        self.stderr.close()
        self.workspace.cleanup()


class _Worker:
    def __init__(self, sock, infile, outfile, pid):
        self.sock = sock
        self.infile = infile
        self.outfile = outfile
        self.pid = pid

    def request(self, command, barrier_port=0, rows=1):
        started = time.perf_counter()
        _write_int(self.outfile, 0)
        self.outfile.write(b"\x01" if barrier_port else b"\x00")
        _write_int(self.outfile, barrier_port or 0)
        conf = {"timezone": "UTC", "PY_EXECUTE_USER": "", "groupId": ""}
        _write_int(self.outfile, len(conf))
        for key, value in conf.items():
            _write_text(self.outfile, key)
            _write_text(self.outfile, value)
        _write_text(self.outfile, command)
        schema = pa.schema([("value", pa.int64())])
        with pa.ipc.new_stream(self.outfile, schema) as writer:
            if rows:
                writer.write_batch(pa.record_batch(
                    [pa.array(list(range(rows)), type=pa.int64())], schema=schema))
        _write_int(self.outfile, SpecialLengths.END_OF_STREAM)
        self.outfile.flush()
        while True:
            flag = _read_int(self.infile)
            if flag == SpecialLengths.START_ARROW_STREAM:
                pa.ipc.open_stream(self.infile).read_all()
            elif flag == SpecialLengths.ARROW_STREAM_CRASH:
                continue
            elif flag == SpecialLengths.PYTHON_EXCEPTION_THROWN:
                length = _read_int(self.infile)
                raise RuntimeError(self.infile.read(length).decode("utf-8", "replace"))
            elif flag == SpecialLengths.END_OF_DATA_SECTION:
                end = _read_int(self.infile)
                if end != SpecialLengths.END_OF_STREAM:
                    raise RuntimeError("worker refused reuse (%s)" % end)
                return (time.perf_counter() - started) * 1000.0
            else:
                raise RuntimeError("unexpected worker flag %s" % flag)

    def close(self):
        try:
            self.infile.close()
        finally:
            try:
                self.outfile.close()
            finally:
                self.sock.close()


class BarrierServer:
    def __init__(self):
        self.calls = 0
        self._lock = threading.Lock()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(128)
        self.port = self.listener.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        while not self._stop.is_set():
            self.listener.settimeout(0.2)
            try:
                conn, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                conn.settimeout(30)
                stream = conn.makefile("rwb")
                function = _read_int(stream)
                if function != 1:
                    _write_text(stream, "Not recognized function call from python side.")
                else:
                    with self._lock:
                        self.calls += 1
                    _write_text(stream, "success")
                stream.flush()
                stream.close()
            finally:
                conn.close()

    def close(self):
        self._stop.set()
        self.listener.close()
        self._thread.join(2)


def _summarize(name, latencies, errors, started, extra):
    elapsed = time.perf_counter() - started
    result = {
        "ok": len(latencies),
        "errors": len(errors),
        "elapsed_s": round(elapsed, 3),
        "p50_ms": None if not latencies else round(_percentile(latencies, 50), 3),
        "p95_ms": None if not latencies else round(_percentile(latencies, 95), 3),
        "max_ms": None if not latencies else round(max(latencies), 3),
    }
    if latencies and elapsed > 0:
        result["per_s"] = round(len(latencies) / elapsed, 2)
    result.update(extra)
    if errors:
        raise RuntimeError("%s failed %s of %s requests: %s" % (
            name, len(errors), len(latencies) + len(errors), errors[0]))
    return result


def _run_pooled(workers, requests, command, barrier_port=0, rows=1):
    daemon = Daemon()
    latencies = []
    errors = []
    lock = threading.Lock()
    fds_before = _descriptor_count()
    started = time.perf_counter()
    try:
        pool = [daemon.connect() for _ in range(min(workers, requests))]

        def work(worker, count):
            local = []
            for _ in range(count):
                local.append(worker.request(command, barrier_port=barrier_port, rows=rows))
            return worker.pid, local

        each, remainder = divmod(requests, len(pool))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(work, worker, each + (i < remainder)) for i, worker in enumerate(pool)]
            for future in futures:
                try:
                    _pid, samples = future.result()
                    with lock:
                        latencies.extend(samples)
                except Exception as error:
                    with lock:
                        errors.append(str(error).splitlines()[0][:500])
        for worker in pool:
            worker.close()
        extra = {"driver_fds_delta": None if fds_before is None else _descriptor_count() - fds_before}
        return latencies, errors, started, extra
    finally:
        daemon.close()


def scenario_worker_reuse(workers, requests, rows):
    command = "import os\ncontext.build_result([{'pid': os.getpid()}])"
    latencies, errors, started, extra = _run_pooled(workers, requests, command, rows=rows)
    return _summarize("worker-reuse", latencies, errors, started, extra)


def scenario_barrier(workers, requests, rows):
    server = BarrierServer()
    try:
        command = "context.barrier()\nimport os\ncontext.build_result([{'pid': os.getpid()}])"
        latencies, errors, started, extra = _run_pooled(
            workers, requests, command, barrier_port=server.port, rows=rows)
        extra["callbacks"] = server.calls
        expected = requests
        if not errors and server.calls != expected:
            errors.append("expected %s barrier callbacks, got %s" % (expected, server.calls))
        return _summarize("barrier", latencies, errors, started, extra)
    finally:
        server.close()


def _read_once(host, port, base, expected_rows):
    started = time.perf_counter()
    sock = socket.create_connection((host, port), timeout=30)
    try:
        stream = sock.makefile("rwb", 1024 * 1024)
        try:
            if _read_int(stream) != SpecialLengths.START_ARROW_STREAM:
                raise RuntimeError("OnceServer did not start an Arrow stream")
            count = 0
            with pa.ipc.open_stream(StrictArrowInput(stream)) as reader:
                for batch in reader:
                    expected = pa.array(range(base + count, base + count + batch.num_rows), type=pa.int64())
                    if not batch.column(0).equals(expected):
                        raise RuntimeError("Partition contents changed")
                    count += batch.num_rows
            if count != expected_rows:
                raise RuntimeError("Partition row count changed")
            end, eos = struct.unpack("!ii", stream.read(8))
            if (end, eos) != (SpecialLengths.END_OF_DATA_SECTION, SpecialLengths.END_OF_STREAM):
                raise RuntimeError("OnceServer end markers were %s %s" % (end, eos))
            _write_int(stream, SpecialLengths.END_OF_STREAM)
            stream.flush()
        finally:
            stream.close()
    finally:
        sock.close()
    return count, (time.perf_counter() - started) * 1000.0


def scenario_spark_ray(workers, requests, rows):
    """`requests` here is the number of partitions, served `workers` at a time."""
    latencies = []
    errors = []
    moved = 0
    started = time.perf_counter()
    remaining = requests
    while remaining > 0:
        width = min(workers, remaining)
        remaining -= width
        servers = []
        threads = []
        expected = []
        for shard in range(width):
            server = OnceServer("127.0.0.1", 0, "UTC")
            host, port = server.bind()
            if not server.is_bind:
                raise RuntimeError("OnceServer failed to bind")
            base = (requests - remaining - width + shard) * rows
            payload = ({"value": i} for i in range(base, base + rows))
            thread = threading.Thread(target=server.serve, args=(payload,))
            thread.start()
            servers.append((server, host, port))
            threads.append(thread)
            expected.append(base)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=width) as executor:
                futures = [executor.submit(_read_once, host, port, base, rows)
                           for (_, host, port), base in zip(servers, expected)]
                for future, want in zip(futures, expected):
                    try:
                        count, elapsed = future.result()
                        latencies.append(elapsed)
                        moved += count
                    except Exception as error:
                        errors.append(str(error).splitlines()[0][:500])
        finally:
            for thread in threads:
                thread.join(2)
            for server, _, _ in servers:
                server.close()
            for thread in threads:
                thread.join(5)
                if thread.is_alive():
                    errors.append("server thread did not stop")
    extra = {"rows": moved}
    if latencies and (time.perf_counter() - started) > 0:
        extra["rows_per_s"] = round(moved / (time.perf_counter() - started), 1)
    return _summarize("spark-ray", latencies, errors, started, extra)


SCENARIOS = {
    "worker-reuse": scenario_worker_reuse,
    "barrier": scenario_barrier,
    "spark-ray": scenario_spark_ray,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Load-test PyJava's worker, barrier, and OnceServer sockets")
    parser.add_argument("--scenario", default="all", choices=["all"] + list(SCENARIOS))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--requests", type=int, default=40)
    parser.add_argument("--rows", type=int, default=20000)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    if not 1 <= args.workers <= 16:
        parser.error("--workers must be from 1 to 16")
    if not 1 <= args.requests <= 400:
        parser.error("--requests must be from 1 to 400")
    if not 1 <= args.rows <= 200000:
        parser.error("--rows must be from 1 to 200000")

    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    report = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu": os.cpu_count(),
        "pyarrow": pa.__version__,
        "workers": args.workers,
        "requests": args.requests,
        "rows": args.rows,
        "note": "Python protocol smoke only; spark-ray is a legacy scenario name. No Spark/Ray engine is started.",
        "scenarios": {},
    }
    exit_code = 0
    for name in names:
        try:
            report["scenarios"][name] = SCENARIOS[name](args.workers, args.requests, args.rows)
        except Exception as error:
            exit_code = 1
            report["scenarios"][name] = {"error": str(error)}
    path = os.path.abspath(args.output)
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
        handle.write("\n")
    print(json.dumps(report, indent=2))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
