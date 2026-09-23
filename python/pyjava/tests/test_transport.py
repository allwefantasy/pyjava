"""Real loopback TCP/Arrow protocol regressions; no Spark or Ray service needed."""
import concurrent.futures
import os
import pathlib
import select
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import pyarrow as pa

from pyjava.utils import local_connect_and_auth

PYTHON_ROOT = str(pathlib.Path(__file__).resolve().parents[2])


def read_int(stream):
    data = stream.read(4)
    if len(data) != 4:
        raise EOFError("worker closed the connection")
    return struct.unpack("!i", data)[0]


def write_int(stream, value):
    stream.write(struct.pack("!i", value))


def write_text(stream, value):
    data = value.encode("utf-8")
    write_int(stream, len(data))
    stream.write(data)


class WorkerConnection:
    def __init__(self, sock):
        self.sock = sock
        self.infile = sock.makefile("rb")
        self.outfile = sock.makefile("wb")

    def close(self):
        self.infile.close()
        self.outfile.close()
        self.sock.close()

    def run(self, command, rows=10, barrier_port=0):
        failures = []

        def send():
            try:
                write_int(self.outfile, 0)
                self.outfile.write(b"\x01" if barrier_port else b"\x00")
                write_int(self.outfile, barrier_port or 0)
                conf = {"timezone": "UTC", "PY_EXECUTE_USER": "", "groupId": ""}
                write_int(self.outfile, len(conf))
                for key, value in conf.items():
                    write_text(self.outfile, key)
                    write_text(self.outfile, value)
                write_text(self.outfile, command)
                schema = pa.schema([("value", pa.int64())])
                with pa.ipc.new_stream(self.outfile, schema) as writer:
                    for start in range(0, rows, 1024):
                        writer.write_batch(pa.record_batch(
                            [pa.array(range(start, min(rows, start + 1024)), type=pa.int64())],
                            schema=schema))
                write_int(self.outfile, -4)
                self.outfile.flush()
            except BaseException as error:
                failures.append(error)

        sender = threading.Thread(target=send, daemon=True)
        sender.start()
        output = []
        try:
            while True:
                flag = read_int(self.infile)
                if flag == -6:
                    with pa.ipc.open_stream(self.infile) as reader:
                        output.extend(reader.read_all().to_pylist())
                elif flag == 0:
                    continue
                elif flag == -2:
                    length = read_int(self.infile)
                    raise RuntimeError(self.infile.read(length).decode("utf-8"))
                elif flag == -1:
                    end = read_int(self.infile)
                    if end != -4:
                        raise RuntimeError("invalid reuse marker: %s" % end)
                    break
                else:
                    raise RuntimeError("unexpected response: %s" % flag)
        finally:
            sender.join(5)
        if sender.is_alive():
            raise AssertionError("input writer did not finish")
        if failures:
            raise failures[0]
        return output


@unittest.skipUnless(hasattr(os, "fork"), "daemon requires fork")
class TransportTests(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix="pyjava-transport-")
        self.stderr = tempfile.TemporaryFile(mode="w+b")
        env = dict(os.environ, PYTHONPATH=PYTHON_ROOT, PY_WORKER_REUSE="1", BUFFER_SIZE="8192")
        self.daemon = subprocess.Popen(
            [sys.executable, "-m", "pyjava.daemon"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.stderr, env=env, cwd=self.workspace.name)
        self.connections = []
        if not select.select([self.daemon.stdout], [], [], 30)[0]:
            self.tearDown()
            self.fail("daemon startup timed out")
        try:
            self.port = read_int(self.daemon.stdout)
        except EOFError:
            self.stderr.seek(0)
            error = self.stderr.read().decode()
            self.tearDown()
            self.fail(error)

    def tearDown(self):
        for connection in self.connections:
            connection.close()
        if self.daemon.poll() is None:
            self.daemon.stdin.close()
            try:
                self.daemon.wait(10)
            except subprocess.TimeoutExpired:
                self.daemon.kill()
                self.daemon.wait(5)
        self.daemon.stdout.close()
        if not self.daemon.stdin.closed:
            self.daemon.stdin.close()
        self.stderr.close()
        self.workspace.cleanup()

    def connect(self):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=10)
        sock.settimeout(20)
        connection = WorkerConnection(sock)
        pid = read_int(connection.infile)
        self.assertGreater(pid, 0)
        self.connections.append(connection)
        return connection, pid

    def test_many_requests_reuse_one_process_without_descriptor_growth(self):
        connection, pid = self.connect()
        descriptor_counts = []
        for _ in range(30):
            result = connection.run(
                "import os\ncontext.build_result([{'pid': os.getpid(), "
                "'fds': len(os.listdir('/dev/fd'))}])")
            self.assertEqual(result[0]["_0"], pid)
            descriptor_counts.append(result[0]["_1"])
        self.assertLessEqual(max(descriptor_counts) - min(descriptor_counts), 2)

    def test_ignored_large_input_is_drained_before_reuse(self):
        connection, pid = self.connect()
        self.assertEqual(connection.run("context.build_result([{'answer': 42}])", 200000),
                         [{"_0": 42}])
        result = connection.run(
            "context.build_result({'value': r['value'] + 1} for r in context.fetch_once_as_rows())")
        self.assertEqual([r["_0"] for r in result], list(range(1, 11)))

    def test_partial_input_is_drained_before_reuse(self):
        connection, _ = self.connect()
        self.assertEqual(connection.run(
            "context.build_result([next(context.fetch_once_as_rows())])", 25000),
            [{"_0": 0}])
        self.assertEqual(connection.run("context.build_result([{'ok': 1}])"), [{"_0": 1}])

    def test_parallel_workers_have_independent_streams(self):
        workers = [self.connect() for _ in range(4)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(conn.run,
                       "import os\ncontext.build_result([{'pid': os.getpid()}])")
                       for conn, _ in workers]
            self.assertEqual([f.result(30)[0]["_0"] for f in futures],
                             [pid for _, pid in workers])

    def test_python_exception_and_crash_do_not_break_daemon(self):
        connection, _ = self.connect()
        with self.assertRaisesRegex(RuntimeError, "intentional"):
            connection.run("raise ValueError('intentional')")
        broken, _ = self.connect()
        with self.assertRaises(EOFError):
            broken.run("import os\nos._exit(7)")
        healthy, _ = self.connect()
        self.assertEqual(healthy.run("context.build_result([{'ok': 1}])"), [{"_0": 1}])

    def test_cancellation_only_kills_the_selected_worker(self):
        first, pid = self.connect()
        other, _ = self.connect()
        write_int(self.daemon.stdin, pid)
        self.daemon.stdin.flush()
        self.assertEqual(first.infile.read(1), b"")
        self.assertEqual(other.run("context.build_result([{'ok': 1}])"), [{"_0": 1}])

    def test_empty_arrow_input(self):
        connection, _ = self.connect()
        result = connection.run(
            "context.build_result([{'count': sum(1 for _ in context.fetch_once_as_rows())}])", 0)
        self.assertEqual(result, [{"_0": 0}])

    def test_barrier_calls_hit_the_callback_and_do_not_stick_to_the_next_task(self):
        calls = []
        stop, port = _barrier_server(calls)
        try:
            connection, pid = self.connect()
            result = connection.run(
                "from pyjava.barrier import BarrierTaskContext\n"
                "context.barrier()\n"
                "BarrierTaskContext.get().barrier()\n"
                "import os\n"
                "context.build_result([{'pid': os.getpid()}])",
                barrier_port=port)
            self.assertEqual(result, [{"_0": pid}])
            self.assertEqual(calls, [1, 1])
            self.assertEqual(connection.run("context.build_result([{'ok': 1}])"), [{"_0": 1}])
            self.assertEqual(calls, [1, 1])
            with self.assertRaisesRegex(RuntimeError, "barrier stage"):
                connection.run("context.barrier()\ncontext.build_result([{'ok': 1}])")
            healthy, _ = self.connect()
            self.assertEqual(healthy.run("context.build_result([{'ok': 1}])"), [{"_0": 1}])
        finally:
            stop()

    def test_java_barrier_failure_fails_only_that_worker(self):
        calls = []
        stop, port = _barrier_server(calls, reply="barrier timed out")
        try:
            connection, _ = self.connect()
            with self.assertRaisesRegex(RuntimeError, "barrier timed out"):
                connection.run("context.barrier()\ncontext.build_result([{'ok': 1}])",
                               barrier_port=port)
            self.assertEqual(calls, [1])
            healthy, _ = self.connect()
            self.assertEqual(healthy.run("context.build_result([{'ok': 1}])"), [{"_0": 1}])
        finally:
            stop()

    def test_parallel_tasks_send_independent_barrier_calls(self):
        calls = []
        stop, port = _barrier_server(calls)
        try:
            workers = [self.connect() for _ in range(4)]
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(conn.run,
                           "context.barrier()\nimport os\ncontext.build_result([{'pid': os.getpid()}])",
                           10, port) for conn, _ in workers]
                pids = [f.result(30)[0]["_0"] for f in futures]
            self.assertEqual(pids, [pid for _, pid in workers])
            self.assertEqual(sorted(calls), [1, 1, 1, 1])
        finally:
            stop()


def _barrier_server(calls, reply="success"):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    port = listener.getsockname()[1]
    stopped = threading.Event()

    def loop():
        while not stopped.is_set():
            listener.settimeout(0.2)
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                conn.settimeout(5)
                with conn.makefile("rwb") as stream:
                    calls.append(read_int(stream))
                    message = reply() if callable(reply) else reply
                    write_text(stream, message)
                    stream.flush()
            finally:
                conn.close()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    def shutdown():
        stopped.set()
        listener.close()
        thread.join(2)

    return shutdown, port


class SimpleWorkerTests(unittest.TestCase):
    @unittest.skipUnless(hasattr(os, "fork"), "daemon requires fork")
    def test_repeated_fork_failure_is_a_negative_code_not_daemon_failure(self):
        import errno
        from pyjava.daemon import fork_worker
        with patch("pyjava.daemon.os.fork", side_effect=OSError(errno.EAGAIN, "busy")) as fork:
            with patch("pyjava.daemon.time.sleep"):
                self.assertEqual(fork_worker(), -errno.EAGAIN)
        self.assertEqual(fork.call_count, 2)

    def test_transfer_socket_disables_nagle_and_honors_buffer_override(self):
        from pyjava.utils import configure_transfer_socket, data_socket_buffer
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            client = socket.socket()
            try:
                configure_transfer_socket(client)
                client.connect(listener.getsockname())
                accepted, _ = listener.accept()
                try:
                    configure_transfer_socket(accepted)
                    self.assertTrue(client.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY))
                    self.assertTrue(accepted.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY))
                    self.assertTrue(client.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE))
                    self.assertEqual(data_socket_buffer(), 1024 * 1024)
                finally:
                    accepted.close()
            finally:
                client.close()

    def test_connection_timeout_is_not_retained_for_file_stream(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            stream, sock = local_connect_and_auth(listener.getsockname()[1])
            peer, _ = listener.accept()
            try:
                self.assertIsNone(sock.gettimeout())
                self.assertTrue(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY))
                self.assertTrue(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE))
            finally:
                stream.close()
                sock.close()
                peer.close()


if __name__ == "__main__":
    unittest.main()
