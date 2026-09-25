import logging
import os
import socket
import traceback
import threading
import time

try:
    import ray
except ImportError:
    ray = None

import pyjava.utils as utils
from pyjava.serializers import \
    write_with_length, \
    write_int, read_int, \
    SpecialLengths, ArrowStreamPandasSerializer

os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"


class SocketNotBindException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class DataServerWithId(object):
    def __init__(self, host, port, server_id):
        self.host = host
        self.port = port
        self.server_id = server_id


class OnceServer(object):
    def __init__(self, host, port, timezone, schema=None):
        from pyjava.transfer import positive_env
        self.host, self.port, self.schema = host, port, schema
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(0.2)
        self.timeout = positive_env("PYJAVA_SOCKET_TIMEOUT_SECONDS", 300)
        self.ack_timeout = positive_env("PYJAVA_ACK_TIMEOUT_SECONDS", 30)
        self.accept_timeout = positive_env("PYJAVA_ACCEPT_TIMEOUT_SECONDS", 300)
        self.is_bind = False
        self._closed = threading.Event()
        self._lock = threading.Lock()
        self._active = None

    def bind(self):
        try:
            self.socket.bind((self.host, self.port))
            self.socket.listen(8)
            self.is_bind = True
            return self.socket.getsockname()
        except BaseException:
            self.close()
            raise

    def close(self):
        self._closed.set()
        with self._lock:
            sockets = [self.socket, self._active]
            for sock in sockets:
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    sock.close()

    def _accept(self, deadline):
        if not self.is_bind:
            raise SocketNotBindException("Invoke bind() before serve()")
        while not self._closed.is_set():
            if time.monotonic() >= deadline:
                raise socket.timeout("Partition reader lease expired")
            try:
                conn, _ = self.socket.accept()
            except socket.timeout:
                continue
            with self._lock:
                if self._closed.is_set():
                    conn.close()
                    raise InterruptedError("Arrow server closed")
                self._active = conn
            return conn
        raise InterruptedError("Arrow server closed")

    def _send(self, conn, write_arrow):
        buffer_size = utils.configure_transfer_socket(conn)
        conn.settimeout(self.timeout)  # bounds lack of I/O progress, not total partition time
        stream = conn.makefile("rwb", buffer_size)
        try:
            try:
                write_arrow(stream)
                write_int(SpecialLengths.END_OF_DATA_SECTION, stream)
                write_int(SpecialLengths.END_OF_STREAM, stream)
                stream.flush()
            except Exception:
                error = traceback.format_exc().encode("utf-8")
                try:
                    write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, stream)
                    write_with_length(error, stream)
                    stream.flush()
                except OSError:
                    pass
                raise
            conn.settimeout(self.ack_timeout)
            if read_int(stream) != SpecialLengths.END_OF_STREAM:
                raise IOError("Invalid Arrow partition acknowledgement")
        finally:
            # Shutdown first so closing a buffered file cannot block trying to flush.
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                stream.close()
            finally:
                conn.close()
                with self._lock:
                    self._active = None

    def serve(self, data):
        from pyjava.transfer import arrow_batches, write_batches
        conn = self._accept(time.monotonic() + self.accept_timeout)
        def write(out):
            batches = iter(arrow_batches(data, self.schema))
            first = next(batches)  # validate before announcing an Arrow stream
            import itertools
            write_int(SpecialLengths.START_ARROW_STREAM, out)
            write_batches(itertools.chain([first], batches), out)
        self._send(conn, write)

    def serve_replayable(self, data):
        """Materialize once, then permit task retries until an explicit close or lease expiry.

        Completed data lives on bounded temporary storage, not in an unbounded Python list.
        A disconnected reader never causes the user's transformation to execute again.
        """
        from pyjava.transfer import ArrowSpool, positive_env
        spool = ArrowSpool(data, self.schema, self._closed.is_set)
        try:
            deadline = time.monotonic() + positive_env("PYJAVA_REPLAY_TTL_SECONDS", 300)
            while not self._closed.is_set():
                try:
                    conn = self._accept(deadline)
                except (socket.timeout, InterruptedError):
                    break
                def write(out):
                    write_int(SpecialLengths.START_ARROW_STREAM, out)
                    spool.copy_to(out)
                try:
                    self._send(conn, write)
                except (OSError, EOFError):
                    if self._closed.is_set():
                        break
                    # Immutable snapshot is still valid for a fresh task attempt.
                    logging.warning("Arrow reader disconnected; snapshot retained until lease expiry")
        finally:
            spool.close()


def _ray_actor(cls):
    if ray is None:
        return cls
    return ray.remote(cls)


@_ray_actor
class RayDataServer(object):

    def __init__(self, server_id, java_server, port=0, timezone="Asia/Harbin"):
        from pyjava.rayfix import RayWrapper
        self.server = OnceServer(
            RayWrapper().get_address(), port, java_server.timezone)
        try:
            (rel_host, rel_port) = self.server.bind()
        except Exception as e:
            print(traceback.format_exc())
            raise e

        self.host = rel_host
        self.port = rel_port
        self.timezone = timezone
        self.server_id = server_id
        self.java_server = java_server
        self.is_dev = utils.is_dev()

    def serve(self, func_for_row=None, func_for_rows=None, func_for_batches=None):
        from pyjava.api.mlsql import RayContext
        try:
            if func_for_batches is not None:
                data = func_for_batches(RayContext.fetch_arrow_batches(self.java_server))
            elif func_for_row is not None:
                data = (func_for_row(item)
                        for item in RayContext.fetch_once_as_rows(self.java_server))
            elif func_for_rows is not None:
                data = func_for_rows(
                    RayContext.fetch_once_as_rows(self.java_server))
            else:
                raise ValueError("Provide a row, row-iterator, or Arrow-batch function")
            self.server.serve_replayable(data)
        except Exception as e:
            logging.error(f"Fail to processing data in  Ray Data Server {self.host}:{self.port}")
            raise e
        finally:
            self.close()

    def close(self):
        try:
            self.server.close()
            if ray is not None:
                ray.actor.exit_actor()
        except Exception:
            print(traceback.format_exc())

    def connect_info(self):
        return DataServerWithId(self.host, self.port, self.server_id)


def _transform(java_server, conf, func_for_row, func_for_rows, func_for_batches):
    from pyjava.api.mlsql import RayContext
    if func_for_batches is not None:
        return func_for_batches(RayContext.fetch_arrow_batches(java_server, conf))
    if func_for_row is not None:
        return (func_for_row(item)
                for item in RayContext.fetch_once_as_rows(java_server, conf))
    if func_for_rows is not None:
        return func_for_rows(RayContext.fetch_once_as_rows(java_server, conf))
    raise ValueError("Provide a row, row-iterator, or Arrow-batch function")


@_ray_actor
class RaySnapshotWorker(object):
    """Resident result actor for ``python.socket.transport=shared``.

    One setup creates a bounded number of these. Each actor owns one
    SnapshotService and many tokens. ``generate`` does not exit the actor;
    the snapshot stays readable after the method returns until its lease is
    running. The actor exits from ``shutdown``, from ``abandon`` when a
    generation misses its prepare deadline, after ``enable_idle`` once every
    token is gone for ``python.ray.snapshot.actor.idle.ms``, or — without
    ``enable_idle`` — after ``python.ray.snapshot.actor.orphan.ms`` of no
    coordinator heartbeat when it has no running lease. That last path covers
    a coordinator process that died before ``enable_idle``: an actor that
    never received ``generate``, and one whose ``generate`` finished but was
    never activated. Exit closes this actor's listener and deletes its
    directory. It never calls ``ray.shutdown``.
    """

    def __init__(self, worker_id, conf=None):
        import os
        from pyjava.rayfix import RayWrapper
        from pyjava.snapshot import _conf_int, get_shared_service, snapshot_actor_workdir
        self.worker_id = worker_id
        self.conf = dict(conf or {})
        self._os = os
        # Exclusive directory. The service then makes its own subdirectory.
        # Failure cleanup deletes this path and does not touch the parent.
        self._workdir = snapshot_actor_workdir(self.conf, worker_id)
        os.makedirs(self._workdir, exist_ok=True)
        service_conf = dict(self.conf)
        service_conf["python.socket.shared.dir"] = self._workdir
        self.service = get_shared_service(RayWrapper().get_address(), 0, service_conf)
        self._idle_s = _conf_int(
            self.conf, "python.ray.snapshot.actor.idle.ms",
            "PYJAVA_SNAPSHOT_ACTOR_IDLE_MS", 60000) / 1000.0
        # Independent of idle. A short idle TTL must not reap an actor that
        # the coordinator has not armed yet, and a long lease must not keep
        # an unactivated actor after the coordinator is gone.
        self._orphan_s = _conf_int(
            self.conf, "python.ray.snapshot.actor.orphan.ms",
            "PYJAVA_SNAPSHOT_ACTOR_ORPHAN_MS", 60000) / 1000.0
        self._inflight = 0
        self._generate_deadline = None
        self._generate_lock = threading.Lock()
        self._lock = threading.Lock()
        self._idle_enabled = False
        self._last_activity = time.monotonic()
        self._stop = threading.Event()
        self._watcher = threading.Thread(target=self._idle_loop, daemon=True,
                                         name="pyjava-snapshot-idle")
        self._prepare_watcher = threading.Thread(
            target=self._prepare_loop, daemon=True, name="pyjava-snapshot-prepare")
        self._watcher.start()
        self._prepare_watcher.start()

    def endpoint(self):
        # A liveness probe must not refresh the idle TTL. The actor exits
        # after its tokens are gone, even if something is still polling it.
        return {"worker_id": self.worker_id, "host": self.service.host,
                "port": self.service.port, "workdir": self._workdir,
                "spool_dir": self.service.spool_dir}

    def generate(self, java_server, func_for_row, func_for_rows, func_for_batches,
                 partition_id, attempt_id):
        """Materialize one partition. The generation slot ends when this returns.

        A callback that never returns is not preempted. ``_prepare_loop`` and
        ``abandon`` end the process instead; this method cannot time itself out
        while it is blocked inside the callback.
        """
        if not self._generate_lock.acquire(blocking=False):
            raise RuntimeError("snapshot actor is already generating")
        try:
            return self._generate(java_server, func_for_row, func_for_rows,
                                  func_for_batches, partition_id, attempt_id)
        finally:
            self._generate_lock.release()

    def _generate(self, java_server, func_for_row, func_for_rows, func_for_batches,
                  partition_id, attempt_id):
        from pyjava.snapshot import (
            PROTOCOL, SharedDataServer, _conf_int, identity_value)
        partition_id = identity_value(partition_id, -1)
        attempt_id = identity_value(attempt_id, -1)
        timezone = getattr(java_server, "timezone", "") or ""
        prepare_ms = _conf_int(self.conf, "python.socket.shared.prepare.timeout.ms",
                               "PYJAVA_SNAPSHOT_PREPARE_MS", self.service.prepare_ms)
        with self._lock:
            self._inflight += 1
            # Actor-local bound from method entry. Scheduling before this
            # method runs is the coordinator's deadline, not this one.
            self._generate_deadline = time.monotonic() + prepare_ms / 1000.0
        token = None
        try:
            token = self.service.register(
                partition_id=partition_id, attempt_id=attempt_id,
                lease_ms=_conf_int(self.conf, "python.socket.shared.lease.ms",
                                   "PYJAVA_SNAPSHOT_LEASE_MS", self.service.lease_ms),
                prepare_ms=prepare_ms,
                activate_ms=_conf_int(self.conf, "python.socket.shared.activate.timeout.ms",
                                      "PYJAVA_SNAPSHOT_ACTIVATE_MS",
                                      _conf_int(self.conf, "python.socket.shared.lease.ms",
                                                "PYJAVA_SNAPSHOT_LEASE_MS",
                                                self.service.activate_ms)))
            data = _transform(java_server, self.conf, func_for_row, func_for_rows,
                              func_for_batches)
            # Lease starts at activate(), after every partition in this setup
            # has been materialized, so early results do not expire mid-setup.
            size = self.service.materialize(token, data, start_lease=False)
            if self.service.status(token).get("state") != "ready":
                raise RuntimeError("snapshot %s was not ready after materialize" % token)
            row = SharedDataServer(
                self.service.host, self.service.port, token, PROTOCOL,
                partition_id, attempt_id, size, 0, timezone).as_row()
            if row["protocol"] != PROTOCOL:
                raise RuntimeError("snapshot protocol was rewritten")
            if row["partition_id"] != partition_id or row["attempt_id"] != attempt_id:
                raise RuntimeError("snapshot identity was rewritten")
            return row
        except Exception as e:
            logging.error("shared snapshot generation failed on %s: %s",
                          self.worker_id, e)
            if token is not None:
                try:
                    self.service.fail(token, "%s: %s" % (type(e).__name__, str(e)[:400]))
                except Exception:
                    pass
            raise
        finally:
            with self._lock:
                self._inflight -= 1
                self._generate_deadline = None
            self._last_activity = time.monotonic()

    def activate(self, tokens):
        """Start leases for tokens this actor owns. Unknown tokens are ignored."""
        if str(self.conf.get("python.ray.snapshot.test.fail_activate", "")).lower() in (
                "1", "true"):
            raise RuntimeError("snapshot activate failed")
        deadlines = {}
        for token in tokens:
            if self.service.entries.get(token) is None:
                continue
            deadlines[token] = self.service.activate(token)
            if deadlines[token] <= 0:
                raise RuntimeError("activated lease deadline must be positive")
        return deadlines

    def keepalive(self):
        self.service.keepalive_deferred()
        self._last_activity = time.monotonic()
        return True

    def release_token(self, token):
        if self.service.entries.get(token) is None:
            return False
        return self.service.release(token)

    def release_all(self):
        for token in list(self.service.entries):
            self.service.release(token)
        return True

    def enable_idle(self):
        """Allow the idle TTL to exit this actor once no token remains."""
        self._last_activity = time.monotonic()
        self._idle_enabled = True
        return True

    def diagnostics(self):
        rows = []
        for token, entry in list(self.service.entries.items()):
            rows.append({
                "token": token, "state": entry.state,
                "lease_ms": entry.lease_ms,
                "lease_deadline_ms": entry.lease_deadline_ms,
                "readers": entry.readers, "writer": entry.writer_active,
            })
        return {
            "idle_enabled": self._idle_enabled,
            "idle_s": self._idle_s,
            "orphan_s": self._orphan_s,
            "inflight": self._inflight,
            "retained": self.service.retained_count(),
            "service_lease_ms": self.service.lease_ms,
            "entries": rows,
        }

    def shutdown(self):
        """Explicit service end. Closes the listener and exits this actor only."""
        self._stop.set()
        try:
            self.service.close()
        except Exception:
            logging.exception("snapshot worker close failed")
        self._drop_workdir()
        self._exit_actor()

    def abandon(self):
        """Drop this actor's files and process while ``generate`` is still blocked.

        Must run on the extra concurrency slot. A stuck native call cannot be
        preempted; exiting the process is the cutoff. Files are removed first
        because a later ``ray.kill`` does not run atexit.
        """
        self._stop.set()
        try:
            self.service.close()
        except Exception:
            logging.exception("snapshot abandon close failed")
        self._drop_workdir()
        self._os._exit(0)

    def _drop_workdir(self):
        import shutil
        path = getattr(self, "_workdir", None)
        if path:
            shutil.rmtree(path, ignore_errors=True)

    def _prepare_loop(self):
        # Runs beside generate. It cannot raise into the blocked thread.
        while not self._stop.wait(0.05):
            with self._lock:
                deadline = self._generate_deadline
                inflight = self._inflight
            if deadline is None or inflight <= 0 or time.monotonic() < deadline:
                continue
            time.sleep(0.05)
            with self._lock:
                deadline = self._generate_deadline
                inflight = self._inflight
            if deadline is None or inflight <= 0 or time.monotonic() < deadline:
                continue
            logging.error("snapshot prepare deadline exceeded on %s; abandoning",
                          self.worker_id)
            self.abandon()

    def _has_running_lease(self):
        """True while a token is activated and its read lease is still in the future."""
        now_ms = time.time() * 1000.0
        for entry in list(self.service.entries.values()):
            lock = getattr(entry, "lock", None)
            if lock is None:
                continue
            with lock:
                if (entry.state == "ready" and entry.lease_deadline_ms
                        and entry.lease_deadline_ms > now_ms):
                    return True
        return False

    def _reap(self):
        """Close this actor's listener, delete its files, and end the process.

        ``service.close`` unlinks snapshot files even if a writer still holds
        the fd. This does not call ``ray.shutdown`` or touch other actors.
        ``exit_actor`` from this watcher thread would not unwind Ray's caller.
        """
        with self._lock:
            if self._stop.is_set():
                return
            self._stop.set()
        try:
            self.service.close()
        except Exception:
            pass
        self._drop_workdir()
        self._os._exit(0)

    def _idle_loop(self):
        while not self._stop.wait(0.2):
            with self._lock:
                busy = self._inflight > 0
            if busy or self._has_running_lease():
                # Prepare deadline owns an in-flight generate. A running lease
                # stays readable until it ends, whether or not idle was armed.
                continue
            retained = self.service.retained_count()
            if self._idle_enabled and retained > 0:
                self._last_activity = time.monotonic()
                continue
            # Armed and empty: the idle TTL. Not armed: the orphan window,
            # including tokens that were generated but never activated.
            limit = self._idle_s if self._idle_enabled and retained == 0 else self._orphan_s
            if time.monotonic() - self._last_activity >= limit:
                self._reap()

    def _exit_actor(self):
        if ray is None:
            self._os._exit(0)
        ray.actor.exit_actor()
