"""Shared snapshot transport regressions over real loopback TCP.

The token-scoped listener must survive probes, isolates partitions by token,
express preparing/ready/failed/expired states, and stay inside its quotas.
"""
import os
import shutil
import socket
import struct
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import pyarrow as pa

from pyjava.api.mlsql import PythonContext, RayContext
from pyjava.snapshot import (
    MAGIC, VERSION, PROTOCOL,
    OP_STATUS, OP_READ, OP_RELEASE,
    ST_READY, ST_PREPARING, ST_FAILED, ST_EXPIRED, ST_UNKNOWN, ST_BUSY,
    SharedDataServer, SnapshotClient, SnapshotError, SnapshotService)


def rows(count, base=0):
    return ({"value": base + i} for i in range(count))


def read_rows(server, conf=None):
    return list(RayContext.fetch_once_as_rows(server, conf))


class SnapshotServiceCase(unittest.TestCase):
    def make_service(self, conf=None):
        service = SnapshotService("127.0.0.1", 0, conf or {})
        self.addCleanup(service.close)
        return service

    def make_client(self, service, io_timeout=30):
        client = SnapshotClient(service.host, service.port, connect_timeout=5,
                                io_timeout=io_timeout)
        self.addCleanup(client.close)
        return client

    def test_probe_then_valid_token_reads(self):
        service = self.make_service()
        for _ in range(3):
            probe = socket.create_connection((service.host, service.port), timeout=3)
            probe.close()
        token = service.register(partition_id=3, attempt_id=0)
        service.materialize(token, rows(100))
        self.assertEqual([r["value"] for r in read_rows(SharedDataServer(
            service.host, service.port, token, PROTOCOL, 3, 0))], list(range(100)))

    def test_status_during_preparing_then_ready(self):
        service = self.make_service()
        token = service.register(partition_id=1, attempt_id=0)

        def slow():
            for i in range(64):
                time.sleep(0.01)
                yield {"value": i}

        done = threading.Thread(target=service.materialize, args=(token, slow()))
        done.start()
        self.addCleanup(done.join, 10)
        client = self.make_client(service)
        status, info = client.status(token)
        while status == ST_PREPARING:
            status, info = client.status(token)
        self.assertEqual(status, ST_READY)
        done.join(10)
        self.assertEqual(info["partitionId"], 1)

    def test_failed_generation_is_visible_not_a_timeout(self):
        service = self.make_service()
        token = service.register(partition_id=7, attempt_id=2)

        def bad():
            yield {"value": 1}
            raise RuntimeError("model exploded")

        with self.assertRaises(RuntimeError):
            service.materialize(token, bad())
        client = self.make_client(service)
        status, info = client.status(token)
        self.assertEqual(status, ST_FAILED)
        self.assertIn("model exploded", info["error"])
        with self.assertRaises(SnapshotError) as ctx:
            read_rows(SharedDataServer(service.host, service.port, token,
                                       PROTOCOL, 7, 2))
        self.assertEqual(ctx.exception.phase, "generate")
        self.assertEqual(ctx.exception.partition_id, 7)
        self.assertNotIn(token, str(ctx.exception))

    def test_unknown_token_and_bad_frames_are_bounded(self):
        service = self.make_service({"python.socket.shared.handshake.timeout": "400"})
        client = self.make_client(service, io_timeout=5)
        status, _ = client.status("no-such-token")
        self.assertEqual(status, ST_UNKNOWN)
        # Garbage magic: the server cannot recover the frame and must close.
        raw = socket.create_connection((service.host, service.port), timeout=5)
        self.addCleanup(raw.close)
        raw.settimeout(5)
        raw.sendall(struct.pack("!iiii", -559038737, 1, OP_STATUS, 4) + b"junk")
        # Closing with unread bytes may surface as RST instead of a clean FIN.
        try:
            closed = raw.recv(16) == b""
        except ConnectionResetError:
            closed = True
        self.assertTrue(closed, "garbage handshake did not close the connection")
        # Half a handshake then silence is bounded by the handshake timeout.
        half = socket.create_connection((service.host, service.port), timeout=5)
        self.addCleanup(half.close)
        half.settimeout(5)
        half.sendall(struct.pack("!i", MAGIC))
        self.assertEqual(half.recv(16), b"")

    def test_lease_expiry_then_release(self):
        service = self.make_service({"python.socket.shared.lease.ms": "300"})
        token = service.register(partition_id=0, attempt_id=0)
        service.materialize(token, rows(8))
        time.sleep(0.6)
        client = self.make_client(service)
        status, _ = client.status(token)
        self.assertEqual(status, ST_EXPIRED)
        with self.assertRaises(SnapshotError):
            client.read(token)

    def test_release_frees_budget_and_blocks_late_reads(self):
        service = self.make_service()
        token = service.register(partition_id=0, attempt_id=0)
        size = service.materialize(token, rows(32))
        self.assertEqual(service.total_bytes, size)
        client = self.make_client(service)
        status, _ = client.release(token)
        self.assertEqual(status, ST_READY)
        self.assertEqual(service.total_bytes, 0)
        status, _ = client.status(token)
        self.assertEqual(status, ST_EXPIRED)
        self.assertFalse(os.path.exists(service.entries[token].path))

    def test_multiple_tokens_share_one_connection_serially(self):
        service = self.make_service()
        token_a = service.register(partition_id=0, attempt_id=0)
        token_b = service.register(partition_id=1, attempt_id=0)
        service.materialize(token_a, rows(10, base=100))
        service.materialize(token_b, rows(10, base=200))
        client = self.make_client(service)
        status, _ = client.status(token_a)
        self.assertEqual(status, ST_READY)
        info, stream = client.read(token_a)
        self.assertEqual(
            [v for b in pa.ipc.open_stream(stream) for v in b.column(0).to_pylist()],
            list(range(100, 110)))
        stream.expect_end()
        info, stream = client.read(token_b)
        self.assertEqual(
            [v for b in pa.ipc.open_stream(stream) for v in b.column(0).to_pylist()],
            list(range(200, 210)))
        stream.expect_end()
        status, _ = client.release(token_a)
        self.assertEqual(status, ST_READY)
        status, _ = client.status(token_a)
        self.assertEqual(status, ST_EXPIRED)
        # The released partition does not disturb the other token on this listener.
        info, stream = client.read(token_b)
        self.assertEqual(
            [v for b in pa.ipc.open_stream(stream) for v in b.column(0).to_pylist()],
            list(range(200, 210)))
        stream.expect_end()

    def test_disconnect_mid_read_then_reread(self):
        service = self.make_service()
        token = service.register(partition_id=0, attempt_id=0)
        service.materialize(token, rows(500))
        partial = SnapshotClient(service.host, service.port, 5, 5)
        _, stream = partial.read(token)
        stream.read(64)
        partial.close()
        self.assertEqual(
            [r["value"] for r in read_rows(
                SharedDataServer(service.host, service.port, token, PROTOCOL))],
            list(range(500)))

    def test_total_quota_rejects_but_keeps_existing_readable(self):
        token_a_size = None
        service = self.make_service({"python.socket.shared.total.maxBytes": "262144"})
        token_a = service.register()
        token_a_size = service.materialize(token_a, rows(64))
        token_b = service.register()
        with self.assertRaises(IOError):
            service.materialize(token_b, rows(100000))
        self.assertTrue(service.total_bytes <= 262144)
        self.assertEqual(
            [r["value"] for r in read_rows(
                SharedDataServer(service.host, service.port, token_a, PROTOCOL))],
            list(range(64)))
        # Budget returns after release and the partition file is gone.
        status, _ = self.make_client(service).release(token_a)
        self.assertEqual(status, ST_READY)
        self.assertEqual(service.total_bytes, 0)

    def test_registration_limit_and_busy_connection_limit(self):
        service = self.make_service({"python.socket.shared.maxSnapshots": "1",
                                     "python.socket.shared.maxConnections": "1"})
        service.register()
        with self.assertRaises(Exception):
            service.register()
        # One idle client occupies the only handler slot; a second sees BUSY.
        client = self.make_client(service)
        busy = SnapshotClient(service.host, service.port, 5, 5)
        self.addCleanup(busy.close)
        status, _ = busy.status("anything")
        self.assertEqual(status, ST_BUSY)

    def test_per_partition_quota(self):
        service = self.make_service({"python.socket.shared.partition.maxBytes": "8192"})
        token = service.register()
        with self.assertRaises(IOError):
            service.materialize(token, rows(4096))

    def test_stuck_preparation_fails_bounded(self):
        service = self.make_service({"python.socket.shared.prepare.timeout.ms": "300"})
        token = service.register()
        time.sleep(0.8)
        status, info = self.make_client(service).status(token)
        self.assertEqual(status, ST_FAILED)

    def test_descriptor_parse_and_result_row(self):
        descriptor = SharedDataServer("h", 1, "t", PROTOCOL, 4, 2, 99, 123, "UTC")
        self.assertEqual(descriptor.as_row(), {
            "host": "h", "port": 1, "token": "t", "protocol": PROTOCOL,
            "partition_id": 4, "attempt_id": 2, "snapshot_bytes": 99,
            "lease_deadline_ms": 123, "timezone": "UTC"})

    def test_zero_partition_and_attempt_stay_zero(self):
        descriptor = SharedDataServer.from_row({
            "host": "127.0.0.1", "port": 1, "token": "t", "protocol": PROTOCOL,
            "partition_id": 0, "attempt_id": 0, "snapshot_bytes": 1,
            "lease_deadline_ms": 5, "timezone": "UTC"})
        self.assertEqual((descriptor.partition_id, descriptor.attempt_id), (0, 0))
        service = self.make_service()
        token = service.register(partition_id=0, attempt_id=0)
        service.materialize(token, rows(1))
        _, info = self.make_client(service).status(token)
        self.assertEqual((info["partitionId"], info["attemptId"]), (0, 0))

    def test_release_with_active_reader_keeps_budget_until_close(self):
        service = self.make_service({"python.socket.shared.total.maxBytes": "1000000"})
        token = service.register(partition_id=0, attempt_id=0)
        size = service.materialize(token, rows(64))
        entry = service.entries[token]
        handle = entry.open_reader()
        try:
            self.assertTrue(service.release(token))
            self.assertEqual(service.total_bytes, size)
            self.assertTrue(os.path.exists(entry.path))
        finally:
            handle.close()
            entry.close_reader()
            service._settle(entry)
        self.assertEqual(service.total_bytes, 0)
        self.assertFalse(os.path.exists(entry.path))

    def test_release_during_prepare_stops_the_writer(self):
        service = self.make_service({"python.socket.shared.total.maxBytes": "8000000"})
        token = service.register(partition_id=0, attempt_id=0)
        started = threading.Event()

        def slow():
            for i in range(100000):
                if i == 0:
                    started.set()
                time.sleep(0.01)
                yield {"value": i}

        errors = []

        def run():
            try:
                service.materialize(token, slow())
            except Exception as exc:
                errors.append(exc)

        worker = threading.Thread(target=run)
        worker.start()
        self.assertTrue(started.wait(5))
        time.sleep(0.05)
        self.assertTrue(service.release(token))
        worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertTrue(errors)
        self.assertEqual(service.total_bytes, 0)

    def test_inflight_limit_fails_instead_of_waiting(self):
        service = self.make_service({"python.ray.inflight.generations": "1"})
        first = service.register()
        second = service.register()
        started = threading.Event()

        def slow():
            started.set()
            time.sleep(0.4)
            yield {"value": 1}

        worker = threading.Thread(target=lambda: service.materialize(first, slow()))
        worker.start()
        self.assertTrue(started.wait(5))
        began = time.monotonic()
        with self.assertRaises(IOError) as ctx:
            service.materialize(second, rows(1))
        self.assertLess(time.monotonic() - began, 0.3)
        self.assertIn("inflight", str(ctx.exception))
        worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(service.total_bytes, service.entries[first].size)

    def test_close_unlinks_file_held_by_blocked_writer(self):
        parent = tempfile.mkdtemp(prefix="pyjava-snapshot-owner-")
        self.addCleanup(shutil.rmtree, parent, True)
        sentinel = os.path.join(parent, "keep")
        with open(sentinel, "w") as handle:
            handle.write("keep")
        service = self.make_service({"python.socket.shared.dir": parent})
        token = service.register(partition_id=0, attempt_id=0)
        started = threading.Event()
        release = threading.Event()

        def blocked():
            started.set()
            release.wait(30)
            yield {"value": 1}

        def run():
            try:
                service.materialize(token, blocked())
            except BaseException:
                pass

        worker = threading.Thread(target=run)
        worker.daemon = True
        worker.start()
        self.assertTrue(started.wait(5))
        entry = service.entries[token]
        self.assertTrue(entry.path and os.path.exists(entry.path))
        self.assertTrue(entry.path.startswith(parent + os.sep))
        self.assertNotEqual(os.path.dirname(entry.path), parent)
        try:
            service.close()
            self.assertFalse(os.path.exists(entry.path))
            self.assertTrue(worker.is_alive())
            with open(sentinel) as handle:
                self.assertEqual(handle.read(), "keep")
            owned = [name for name in os.listdir(parent) if name != "keep"]
            self.assertEqual(owned, [])
        finally:
            release.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())

    def test_close_unblocks_a_handler_without_waiting_out_the_timeout(self):
        service = SnapshotService("127.0.0.1", 0, {
            "python.socket.shared.handshake.timeout": "300000"})
        raw = socket.create_connection((service.host, service.port), timeout=5)
        raw.sendall(struct.pack("!i", MAGIC))
        began = time.monotonic()
        service.close()
        raw.settimeout(2)
        try:
            raw.recv(16)
        except OSError:
            pass
        self.assertLess(time.monotonic() - began, 2.0)
        raw.close()

    def test_abandoned_read_is_not_reused(self):
        service = self.make_service()
        token = service.register(partition_id=0, attempt_id=0)
        service.materialize(token, rows(32))
        client = SnapshotClient(service.host, service.port, 5, 5)
        self.addCleanup(client.close)
        _, stream = client.read(token)
        stream.read(8)
        with self.assertRaises(SnapshotError) as ctx:
            client.status(token)
        self.assertIn("not reused", str(ctx.exception))

    def test_process_budget_conflict_is_rejected(self):
        from pyjava.snapshot import get_shared_service
        conf = {"python.socket.shared.name": "py-budget-conflict"}
        service = get_shared_service("127.0.0.1", 0, conf)
        self.addCleanup(service.close)
        with self.assertRaises(ValueError) as ctx:
            get_shared_service("127.0.0.1", 0, {
                "python.socket.shared.name": "py-budget-conflict",
                "python.socket.shared.total.maxBytes": "1024"})
        self.assertIn("total.maxBytes", str(ctx.exception))
        self.assertEqual(service.total_max, 4 * 1024 ** 3)

    def test_shared_mode_refuses_legacy_socket_rows(self):
        with self.assertRaises(ValueError) as ctx:
            RayContext._to_data_server(
                {"host": "127.0.0.1", "port": 1, "timezone": "UTC"},
                {"python.socket.transport": "shared"})
        self.assertIn("legacy", str(ctx.exception))


class RayContextSharedCase(unittest.TestCase):
    def _context(self, items):
        batch = pa.record_batch([pa.array([i["value"] for i in items], type=pa.int64()),
                                 pa.array([i["host"] for i in items]),
                                 pa.array([i["port"] for i in items], type=pa.int64()),
                                 pa.array([i["token"] for i in items]),
                                 pa.array([i["protocol"] for i in items]),
                                 pa.array([i["partition_id"] for i in items], type=pa.int64()),
                                 pa.array([i["attempt_id"] for i in items], type=pa.int64()),
                                 pa.array([i["snapshot_bytes"] for i in items], type=pa.int64()),
                                 pa.array([i["lease_deadline_ms"] for i in items], type=pa.int64()),
                                 pa.array([i["timezone"] for i in items])],
                                names=["value", "host", "port", "token", "protocol",
                                       "partition_id", "attempt_id", "snapshot_bytes",
                                       "lease_deadline_ms", "timezone"])
        return PythonContext("ctx", iter([batch]), {"pythonMode": "ray",
                                                    "python.socket.transport": "shared"})

    def test_raycontext_builds_shared_data_servers(self):
        items = [{"value": 0, "host": "127.0.0.1", "port": 9999, "token": "tok",
                  "protocol": PROTOCOL, "partition_id": 5, "attempt_id": 1,
                  "snapshot_bytes": 10, "lease_deadline_ms": 42, "timezone": "UTC"}]
        context = self._context(items)
        server = context.rayContext.data_servers()[0]
        self.assertIsInstance(server, SharedDataServer)
        self.assertEqual((server.token, server.partition_id, server.attempt_id,
                          server.snapshot_bytes, server.lease_deadline_ms),
                         ("tok", 5, 1, 10, 42))

    def test_shared_fetch_round_trips_rows(self):
        service = SnapshotService("127.0.0.1", 0, {})
        self.addCleanup(service.close)
        token = service.register(partition_id=9, attempt_id=0)
        service.materialize(token, rows(200))
        items = [{"value": 0, "host": service.host, "port": service.port, "token": token,
                  "protocol": PROTOCOL, "partition_id": 9, "attempt_id": 0,
                  "snapshot_bytes": -1, "lease_deadline_ms": 0, "timezone": "UTC"}]
        context = self._context(items)
        self.assertEqual([r["value"] for r in context.rayContext.collect()], list(range(200)))


if __name__ == "__main__":
    unittest.main()
