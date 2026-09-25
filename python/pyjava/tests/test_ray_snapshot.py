"""Ray actor lifecycle for the shared snapshot pool.

Owns a private local runtime (``address='local'`` and a private ``_temp_dir``).
If Ray is already connected, the class refuses to start and does not call
``ray.shutdown`` on that runtime. From the repo root::

    PYTHONPATH=python target/transport-venv/bin/python -m unittest \\
        pyjava.tests.test_ray_snapshot -v
"""
import os
import shutil
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import pyarrow as pa

try:
    import ray
except ImportError:
    ray = None

from pyjava.api.mlsql import PythonContext, RayContext
from pyjava.snapshot import PROTOCOL, SharedPrepareTimeout, SnapshotService


def _identity(batches):
    return batches


def _slow(probe):
    def _run(batches):
        import ray as ray_mod
        ray_mod.get(probe.enter.remote())
        try:
            time.sleep(0.45)
            return batches
        finally:
            ray_mod.get(probe.leave.remote())

    return _run


def _context(servers, conf):
    batch = pa.record_batch([
        pa.array([item["host"] for item in servers]),
        pa.array([item["port"] for item in servers], type=pa.int64()),
        pa.array([item["token"] for item in servers]),
        pa.array([item["protocol"] for item in servers]),
        pa.array([item["partition_id"] for item in servers], type=pa.int64()),
        pa.array([item["attempt_id"] for item in servers], type=pa.int64()),
        pa.array([item["snapshot_bytes"] for item in servers], type=pa.int64()),
        pa.array([item["lease_deadline_ms"] for item in servers], type=pa.int64()),
        pa.array([item["timezone"] for item in servers]),
    ], names=["host", "port", "token", "protocol", "partition_id", "attempt_id",
              "snapshot_bytes", "lease_deadline_ms", "timezone"])
    merged = {"pythonMode": "ray", "dataMode": "data",
              "python.socket.transport": "shared"}
    merged.update(conf)
    return PythonContext("ray-snapshot", iter([batch]), merged)


def _port_closed(host, port):
    sock = socket.socket()
    sock.settimeout(0.3)
    try:
        sock.connect((host, int(port)))
    except OSError:
        return True
    else:
        return False
    finally:
        sock.close()


def _orphan_coordinator(argv):
    """Real shared-snapshot setup. The parent SIGKILLs this process.

    Do not call ``ray.shutdown``. The runtime belongs to the test process.
    The pause is the first single-ref ``ray.get`` of a finished ``generate``.
    That runs before the coordinator submits the next partition, so one actor
    has a materialized token and the other has never been asked to generate.
    ``activate`` and ``enable_idle`` have not been called. SIGKILL skips every
    ``finally``, including ``_abort_snapshot_pool``.
    """
    address, namespace, group, parent, flag, orphan_ms, source_dir = argv[1:8]
    import ray
    from pyjava.snapshot import PROTOCOL, SnapshotService

    ray.init(address=address, namespace=namespace, logging_level="ERROR")
    real_get = ray.get
    state = {"paused": False}

    def _wrapped(refs, *args, **kwargs):
        # collect() fetches one finished generate with ray.get(ref). Lists are
        # endpoint / activate. Pausing here is before the next submit().
        if (not state["paused"] and kwargs.get("timeout") is None
                and not isinstance(refs, (list, tuple))):
            state["paused"] = True
            value = real_get(refs)
            names = ["pyjava-snapshot-%s-%d" % (group, index) for index in range(2)]
            actors = [ray.get_actor(name) for name in names]
            real_get([actor.keepalive.remote() for actor in actors], timeout=5)
            with open(flag, "w") as handle:
                handle.write("generated\n")
                handle.flush()
                os.fsync(handle.fileno())
            time.sleep(3600)
            return value
        return real_get(refs, *args, **kwargs)

    ray.get = _wrapped
    os.makedirs(source_dir, exist_ok=True)
    service = SnapshotService("127.0.0.1", 0, {
        "python.socket.shared.dir": source_dir,
        "python.socket.shared.name": "orphan-source-%s" % group,
    })
    try:
        servers = []
        for partition_id in (0, 1):
            token = service.register(partition_id=partition_id, attempt_id=0)
            service.materialize(token, ({"value": value} for value in (partition_id,)))
            servers.append({
                "host": service.host, "port": service.port, "token": token,
                "protocol": PROTOCOL, "partition_id": partition_id, "attempt_id": 0,
                "snapshot_bytes": -1, "lease_deadline_ms": 0, "timezone": "UTC",
            })
        context = _context(servers, {
            "python.ray.snapshot.group": group,
            "python.ray.inflight.generations": "1",
            "python.ray.snapshot.actors": "2",
            "python.socket.shared.dir": parent,
            "python.socket.shared.lease.ms": "120000",
            "python.socket.shared.activate.timeout.ms": "120000",
            "python.socket.shared.prepare.timeout.ms": "120000",
            "python.ray.snapshot.actor.idle.ms": "120000",
            "python.ray.snapshot.actor.orphan.ms": orphan_ms,
        })
        context.rayContext.setup(None, None, _identity)
    finally:
        try:
            service.close()
        except Exception:
            pass


def _peak_actor():
    @ray.remote(num_cpus=0)
    class Peak:
        def __init__(self):
            self.current = 0
            self.peak = 0

        def enter(self):
            self.current += 1
            if self.current > self.peak:
                self.peak = self.current
            return self.current

        def leave(self):
            if self.current > 0:
                self.current -= 1
            return self.current

        def peak(self):
            return self.peak

    return Peak


@unittest.skipUnless(ray is not None, "ray is not installed in this interpreter")
class RaySnapshotActorCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if ray.is_initialized():
            raise RuntimeError(
                "test_ray_snapshot refuses an already connected Ray runtime "
                "(RAY_ADDRESS or another ray.init). It will not call "
                "ray.shutdown on that runtime.")
        cls._owns_runtime = False
        # Default macOS temp paths are already long; Ray's plasma socket must
        # stay under the AF_UNIX limit, so the private directory is under /tmp.
        cls._ray_temp = tempfile.mkdtemp(prefix="pjray", dir="/tmp")
        try:
            ray.init(address="local", num_cpus=4, include_dashboard=False,
                     namespace="pyjava-snapshot-test", logging_level="ERROR",
                     _temp_dir=cls._ray_temp)
            cls._owns_runtime = True
            cls.peak_cls = _peak_actor()
        except BaseException:
            shutil.rmtree(cls._ray_temp, ignore_errors=True)
            if cls._owns_runtime:
                ray.shutdown()
                cls._owns_runtime = False
            raise

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "_owns_runtime", False):
            try:
                ray.shutdown()
            except Exception:
                pass
            cls._owns_runtime = False
        temp = getattr(cls, "_ray_temp", None)
        if temp:
            shutil.rmtree(temp, ignore_errors=True)

    def setUp(self):
        self.service = SnapshotService("127.0.0.1", 0, {})
        self.group = "ray-snapshot-%s" % time.time_ns()
        self._peaks = []

    def tearDown(self):
        self.service.close()
        self._kill_group()
        for actor in self._peaks:
            try:
                ray.kill(actor, no_restart=True)
            except Exception:
                pass

    def _actor_names(self, count):
        return ["pyjava-snapshot-%s-%d" % (self.group, index) for index in range(count)]

    def _kill_group(self):
        if ray is None or not ray.is_initialized():
            return
        for name in self._actor_names(8):
            try:
                actor = ray.get_actor(name)
            except Exception:
                continue
            try:
                ray.kill(actor, no_restart=True)
            except Exception:
                pass

    def _assert_actor_gone(self, name):
        deadline = time.monotonic() + 8
        last = None
        while time.monotonic() < deadline:
            try:
                last = ray.get_actor(name)
            except ValueError:
                return
            time.sleep(0.1)
        self.fail("snapshot actor %s is still registered: %s" % (name, last))

    def _assert_only_sentinel(self, parent):
        found = []
        for root, _dirs, files in os.walk(parent):
            for name in files:
                found.append(os.path.join(root, name))
        self.assertEqual(found, [os.path.join(parent, "keep")])

    def _parent(self):
        parent = tempfile.mkdtemp(prefix="pyjava-snap-case-")
        self.addCleanup(shutil.rmtree, parent, True)
        with open(os.path.join(parent, "keep"), "w") as handle:
            handle.write("keep")
        return parent

    def _run_bounded(self, fn, seconds):
        box = {}

        def wrap():
            try:
                box["value"] = fn()
            except BaseException as exc:
                box["error"] = exc

        thread = threading.Thread(target=wrap, name="pyjava-test-deadline")
        thread.daemon = True
        thread.start()
        thread.join(seconds)
        if thread.is_alive():
            self._kill_group()
            thread.join(3)
            self.fail("call did not return within %ss" % seconds)
        if "error" in box:
            raise box["error"]
        return box.get("value")

    def _source(self, partition_id, attempt_id, values):
        token = self.service.register(partition_id=partition_id, attempt_id=attempt_id)
        self.service.materialize(token, ({"value": value} for value in values))
        return {
            "host": self.service.host, "port": self.service.port, "token": token,
            "protocol": PROTOCOL, "partition_id": partition_id,
            "attempt_id": attempt_id, "snapshot_bytes": -1,
            "lease_deadline_ms": 0, "timezone": "UTC",
        }

    def test_snapshot_stays_readable_after_generate_returns(self):
        source = self._source(0, 0, list(range(20)))
        context = _context([source], {
            "python.ray.snapshot.group": self.group,
            "python.ray.inflight.generations": "1",
            "python.ray.snapshot.actors": "1",
            "python.socket.shared.lease.ms": "20000",
            "python.ray.snapshot.actor.idle.ms": "20000",
        })
        descriptors = context.rayContext.setup(None, None, _identity)
        self.assertEqual(len(descriptors), 1)
        row = descriptors[0]
        self.assertEqual(row.protocol, PROTOCOL)
        self.assertEqual((row.partition_id, row.attempt_id), (0, 0))
        self.assertGreater(row.lease_deadline_ms, 0)
        actor = ray.get_actor("pyjava-snapshot-%s-0" % self.group)
        endpoint = ray.get(actor.endpoint.remote(), timeout=15)
        self.assertEqual(endpoint["port"], row.port)
        seen = [item["value"] for item in RayContext.fetch_once_as_rows(row)]
        again = [item["value"] for item in RayContext.fetch_once_as_rows(row)]
        self.assertEqual(seen, list(range(20)))
        self.assertEqual(again, seen)

    def test_few_actors_and_inflight_cap(self):
        sources = [self._source(index, 0, [index]) for index in range(4)]
        probe = self.peak_cls.remote()
        self._peaks.append(probe)
        context = _context(sources, {
            "python.ray.snapshot.group": self.group,
            "python.ray.inflight.generations": "2",
            "python.ray.snapshot.actors": "4",
            "python.socket.shared.lease.ms": "30000",
            "python.ray.snapshot.actor.idle.ms": "30000",
        })
        descriptors = context.rayContext.setup(None, None, _slow(probe))
        self.assertEqual(len(descriptors), 4)
        self.assertEqual(len({item.port for item in descriptors}), 4)
        self.assertLessEqual(ray.get(probe.peak.remote()), 2)
        self.assertGreaterEqual(ray.get(probe.peak.remote()), 1)
        self.assertEqual(sorted(item.partition_id for item in descriptors), [0, 1, 2, 3])
        values = []
        for item in descriptors:
            values.extend(row["value"] for row in RayContext.fetch_once_as_rows(item))
        self.assertEqual(sorted(values), [0, 1, 2, 3])

    def test_several_inputs_share_few_actors(self):
        sources = [self._source(index, 0, [index]) for index in range(6)]
        context = _context(sources, {
            "python.ray.snapshot.group": self.group,
            "python.ray.inflight.generations": "2",
            "python.socket.shared.lease.ms": "20000",
            "python.ray.snapshot.actor.idle.ms": "20000",
        })
        descriptors = context.rayContext.setup(None, None, _identity)
        self.assertEqual(len({item.port for item in descriptors}), 2)
        self.assertEqual(sorted(item.partition_id for item in descriptors), list(range(6)))

    def test_idle_ttl_exits_the_actor_after_the_lease(self):
        source = self._source(0, 0, [1])
        context = _context([source], {
            "python.ray.snapshot.group": self.group,
            "python.ray.inflight.generations": "1",
            "python.ray.snapshot.actors": "1",
            "python.socket.shared.lease.ms": "400",
            "python.socket.shared.prepare.timeout.ms": "15000",
            "python.socket.shared.activate.timeout.ms": "15000",
            "python.socket.shared.expired.grace.ms": "200",
            "python.ray.snapshot.actor.idle.ms": "600",
        })
        descriptors = context.rayContext.setup(None, None, _identity)
        self.assertEqual(descriptors[0].partition_id, 0)
        name = "pyjava-snapshot-%s-0" % self.group
        actor = ray.get_actor(name)
        armed = ray.get(actor.diagnostics.remote(), timeout=10)
        self.assertTrue(armed["idle_enabled"])
        self.assertEqual(armed["entries"][0]["lease_ms"], 400)
        deadline = time.monotonic() + 8
        gone = False
        last = armed
        while time.monotonic() < deadline:
            try:
                actor = ray.get_actor(name)
                last = ray.get(actor.endpoint.remote(), timeout=1)
            except Exception:
                gone = True
                break
            time.sleep(0.2)
        self.assertTrue(gone, "snapshot actor still answered after lease and idle TTL: %s" % last)

    def test_each_generation_has_its_own_prepare_deadline(self):
        sources = [self._source(index, 0, [index]) for index in range(2)]
        parent = self._parent()
        context = _context(sources, {
            "python.ray.snapshot.group": self.group,
            "python.ray.inflight.generations": "1",
            "python.ray.snapshot.actors": "1",
            "python.socket.shared.dir": parent,
            "python.socket.shared.prepare.timeout.ms": "2500",
            "python.socket.shared.lease.ms": "20000",
            "python.ray.snapshot.actor.idle.ms": "20000",
        })

        def _pause(batches):
            time.sleep(1.5)
            return batches

        began = time.monotonic()
        descriptors = self._run_bounded(
            lambda: context.rayContext.setup(None, None, _pause), 20)
        elapsed = time.monotonic() - began
        self.assertGreater(elapsed, 2.5)
        self.assertLess(elapsed, 12)
        self.assertEqual([item.partition_id for item in descriptors], [0, 1])
        actor = ray.get_actor(self._actor_names(1)[0])
        endpoint = ray.get(actor.endpoint.remote(), timeout=10)
        self.assertEqual(endpoint["port"], descriptors[0].port)

    def test_blocked_generator_prepare_timeout_cleans_actor_and_files(self):
        source = self._source(0, 0, [1])
        parent = self._parent()
        context = _context([source], {
            "python.ray.snapshot.group": self.group,
            "python.ray.inflight.generations": "1",
            "python.ray.snapshot.actors": "1",
            "python.socket.shared.dir": parent,
            "python.socket.shared.prepare.timeout.ms": "1500",
            "python.socket.shared.lease.ms": "20000",
            "python.ray.snapshot.actor.idle.ms": "20000",
        })

        @ray.remote(num_cpus=0)
        class Bystander:
            def ping(self):
                return "model"

        bystander = Bystander.remote()
        self._peaks.append(bystander)

        def _never_first(_batches):
            time.sleep(3600)
            yield {"value": 1}

        began = time.monotonic()
        with self.assertRaises(SharedPrepareTimeout) as ctx:
            self._run_bounded(
                lambda: context.rayContext.setup(None, None, _never_first), 20)
        self.assertLess(time.monotonic() - began, 12)
        self.assertEqual(ctx.exception.partition_id, 0)
        self.assertEqual(ctx.exception.attempt_id, 0)
        self.assertIn("python.socket.shared.prepare.timeout.ms=1500", str(ctx.exception))
        self._assert_actor_gone(self._actor_names(1)[0])
        self._assert_only_sentinel(parent)
        self.assertEqual(ray.get(bystander.ping.remote(), timeout=10), "model")

    def test_model_error_does_not_wait_on_a_busy_callback(self):
        sources = [self._source(index, 7, [index]) for index in range(2)]
        parent = self._parent()
        context = _context(sources, {
            "python.ray.snapshot.group": self.group,
            "python.ray.inflight.generations": "2",
            "python.ray.snapshot.actors": "2",
            "python.socket.shared.dir": parent,
            "python.socket.shared.prepare.timeout.ms": "30000",
            "python.socket.shared.lease.ms": "20000",
            "python.ray.snapshot.actor.idle.ms": "20000",
        })

        def _mixed(batches):
            table = pa.Table.from_batches(list(batches))
            value = table.column("value")[0].as_py()
            if value == 0:
                raise RuntimeError("model marker boom")
            time.sleep(3600)
            yield {"value": value}

        began = time.monotonic()
        with self.assertRaises(Exception) as ctx:
            self._run_bounded(lambda: context.rayContext.setup(None, None, _mixed), 20)
        self.assertLess(time.monotonic() - began, 12)
        self.assertNotIsInstance(ctx.exception, SharedPrepareTimeout)
        self.assertIn("model marker boom", str(ctx.exception))
        for name in self._actor_names(2):
            self._assert_actor_gone(name)
        self._assert_only_sentinel(parent)

    def test_activate_failure_recycles_the_pool(self):
        source = self._source(3, 1, [9])
        parent = self._parent()
        context = _context([source], {
            "python.ray.snapshot.group": self.group,
            "python.ray.inflight.generations": "1",
            "python.ray.snapshot.actors": "1",
            "python.socket.shared.dir": parent,
            "python.ray.snapshot.test.fail_activate": "true",
            "python.socket.shared.prepare.timeout.ms": "15000",
            "python.socket.shared.lease.ms": "20000",
            "python.ray.snapshot.actor.idle.ms": "20000",
        })
        began = time.monotonic()
        with self.assertRaises(Exception) as ctx:
            self._run_bounded(
                lambda: context.rayContext.setup(None, None, _identity), 20)
        self.assertLess(time.monotonic() - began, 12)
        self.assertIn("snapshot activate failed", str(ctx.exception))
        self.assertNotIn("snapshot pool cleanup failed", str(ctx.exception))
        self._assert_actor_gone(self._actor_names(1)[0])
        self._assert_only_sentinel(parent)

    def test_keyboard_interrupt_recycles_the_pool(self):
        source = self._source(1, 2, [4])
        parent = self._parent()
        context = _context([source], {
            "python.ray.snapshot.group": self.group,
            "python.ray.inflight.generations": "1",
            "python.ray.snapshot.actors": "1",
            "python.socket.shared.dir": parent,
            "python.socket.shared.prepare.timeout.ms": "15000",
            "python.socket.shared.lease.ms": "20000",
            "python.ray.snapshot.actor.idle.ms": "20000",
        })

        def _never_first(_batches):
            time.sleep(3600)
            yield {"value": 1}

        real_wait = ray.wait

        def _wait(refs, *args, **kwargs):
            # Let the coordinator observe the running generation, then cancel.
            if kwargs.get("timeout", args[1] if len(args) > 1 else None) != 2:
                raise KeyboardInterrupt()
            return real_wait(refs, *args, **kwargs)

        began = time.monotonic()
        with patch("ray.wait", _wait):
            with self.assertRaises(KeyboardInterrupt):
                self._run_bounded(
                    lambda: context.rayContext.setup(None, None, _never_first), 20)
        self.assertLess(time.monotonic() - began, 12)
        self._assert_actor_gone(self._actor_names(1)[0])
        self._assert_only_sentinel(parent)

    def test_coordinator_process_death_reaps_unactivated_and_unstarted_actors(self):
        import signal
        import subprocess
        import sys
        parent = self._parent()
        source_dir = tempfile.mkdtemp(prefix="pyjava-orphan-src-")
        self.addCleanup(shutil.rmtree, source_dir, True)
        flag = tempfile.mktemp(prefix="pyjava-orphan-flag-")
        self.addCleanup(lambda: os.path.exists(flag) and os.remove(flag))

        @ray.remote(num_cpus=0)
        class Sentinel:
            def ping(self):
                return "model"

        sentinel = Sentinel.remote()
        self._peaks.append(sentinel)
        address = ray.get_runtime_context().gcs_address
        env = os.environ.copy()
        proc = subprocess.Popen(
            [sys.executable, "-c",
             "from pyjava.tests.test_ray_snapshot import _orphan_coordinator; "
             "import sys; _orphan_coordinator(sys.argv)",
             address, "pyjava-snapshot-test", self.group, parent, flag, "15000",
             source_dir],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        names = self._actor_names(2)
        try:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                if os.path.exists(flag) and os.path.getsize(flag) > 0:
                    break
                if proc.poll() is not None:
                    err = proc.stderr.read().decode("utf-8", "replace")
                    self.fail("coordinator exited %s before activate: %s" % (
                        proc.returncode, err[-2000:]))
                time.sleep(0.1)
            else:
                self.fail("coordinator did not reach activate")
            finished = ray.get_actor(names[0])
            unstarted = ray.get_actor(names[1])
            finished_info = ray.get(finished.diagnostics.remote(), timeout=10)
            unstarted_info = ray.get(unstarted.diagnostics.remote(), timeout=10)
            self.assertFalse(finished_info["idle_enabled"])
            self.assertFalse(unstarted_info["idle_enabled"])
            self.assertEqual(finished_info["orphan_s"], 15)
            self.assertEqual(unstarted_info["orphan_s"], 15)
            self.assertGreaterEqual(len(finished_info["entries"]), 1)
            self.assertEqual(int(finished_info["entries"][0]["lease_deadline_ms"]), 0)
            self.assertEqual(unstarted_info["entries"], [])
            self.assertEqual(unstarted_info["inflight"], 0)
            finished_ep = ray.get(finished.endpoint.remote(), timeout=10)
            unstarted_ep = ray.get(unstarted.endpoint.remote(), timeout=10)
            self.assertTrue(os.path.isdir(finished_ep["workdir"]), finished_ep)
            self.assertTrue(os.path.isdir(unstarted_ep["workdir"]), unstarted_ep)
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)
            began = time.monotonic()
            # Orphan window is 15s from the keepalive just before the flag.
            # The 120s lease must not be what we are waiting on.
            deadline = began + 22
            ready = False
            while time.monotonic() < deadline:
                missing = 0
                for name in names:
                    try:
                        ray.get_actor(name)
                    except ValueError:
                        missing += 1
                files_gone = (not os.path.exists(finished_ep["workdir"])
                              and not os.path.exists(unstarted_ep["workdir"]))
                ports_gone = (_port_closed(finished_ep["host"], finished_ep["port"])
                              and _port_closed(unstarted_ep["host"], unstarted_ep["port"]))
                if missing == 2 and files_gone and ports_gone:
                    ready = True
                    break
                time.sleep(0.2)
            elapsed = time.monotonic() - began
            self.assertTrue(ready, "actors still registered or files/listeners left after %.1fs" % elapsed)
            self.assertLess(elapsed, 20, "reap waited %.1fs after SIGKILL; orphan window is 15s and lease is 120s" % elapsed)
            self._assert_only_sentinel(parent)
            self.assertEqual(ray.get(sentinel.ping.remote(), timeout=10), "model")
        finally:
            if proc.poll() is None:
                os.kill(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
