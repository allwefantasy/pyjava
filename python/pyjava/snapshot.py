"""Token-scoped Arrow partition snapshots on a shared, process-level listener.

Wire contract ``pyjava-arrow-snapshot/1`` (mirrors ArrowSnapshotService.scala):

Request::
    int32 MAGIC(0x50594A53) | int32 VERSION(1) | int32 OP | int32 tokenLen | token
    OP: 1=STATUS 2=READ 3=RELEASE
Response::
    int32 STATUS | int32 infoLen | UTF-8 JSON info
    STATUS: 0=READY 1=PREPARING 2=FAILED 3=EXPIRED 4=UNKNOWN_TOKEN 5=BUSY 6=BAD_REQUEST
    READ with STATUS==0 appends: int64 length | snapshot bytes (legacy Arrow IPC)

A connection may carry many sequential requests for different tokens; the server
keeps it open until the client closes, a frame is malformed, or a timeout hits.
Bare connect/close, incomplete handshakes and wrong tokens never consume a
snapshot. READ only transfers while the snapshot state is ready.
"""
import io
import json
import logging
import os
import secrets
import shutil
import socket
import struct
import tempfile
import threading
import time

MAGIC = 0x50594A53
VERSION = 1
PROTOCOL = "pyjava-arrow-snapshot/1"

OP_STATUS = 1
OP_READ = 2
OP_RELEASE = 3

ST_READY = 0
ST_PREPARING = 1
ST_FAILED = 2
ST_EXPIRED = 3
ST_UNKNOWN = 4
ST_BUSY = 5
ST_BAD = 6

_STATUS_NAMES = {ST_READY: "ready", ST_PREPARING: "preparing", ST_FAILED: "failed",
                 ST_EXPIRED: "expired", ST_UNKNOWN: "unknown_token", ST_BUSY: "busy",
                 ST_BAD: "bad_request"}

_DESCRIPTOR_FIELDS = ("host", "port", "token", "protocol", "partition_id",
                       "attempt_id", "snapshot_bytes", "lease_deadline_ms",
                       "timezone")

_MAX_TOKEN = 256
_MAX_INFO = 65536


def _conf_number(conf, key, env, default, scale=1.0):
    """Resolve a numeric setting: conf map first, then env, then default."""
    value = None
    if conf:
        value = conf.get(key)
    if value is None:
        value = os.environ.get(env)
    if value is None:
        return default
    number = float(value) * scale
    if number <= 0:
        raise ValueError("%s/%s must be positive" % (key, env))
    return number


def _conf_int(conf, key, env, default):
    return int(_conf_number(conf, key, env, default, 1.0))


def _conf_ms(conf, key, env, default_seconds):
    """Conf entries are milliseconds; environment fallbacks are seconds."""
    if conf and conf.get(key) is not None:
        return float(conf[key]) / 1000.0
    return _conf_number(conf, key, env, default_seconds)


class SharedPrepareTimeout(TimeoutError):
    """One submitted generation missed ``python.socket.shared.prepare.timeout.ms``.

    The deadline starts when that generation is submitted. It covers Ray
    scheduling, the model transform, and materialize. It is not one budget for
    the whole table, and it does not include activate or the read lease.
    """

    def __init__(self, partition_id, attempt_id, timeout_ms):
        self.partition_id = int(partition_id)
        self.attempt_id = int(attempt_id)
        self.timeout_ms = int(timeout_ms)
        super(SharedPrepareTimeout, self).__init__(
            "snapshot generation exceeded "
            "python.socket.shared.prepare.timeout.ms=%s "
            "partition_id=%s attempt_id=%s; "
            "deadline covers Ray scheduling, model transform, and materialize "
            "for this generation only" %
            (self.timeout_ms, self.partition_id, self.attempt_id))


def snapshot_actor_workdir(conf, worker_id):
    """Exclusive directory for one snapshot actor. Callers do not delete the parent.

    ``python.socket.shared.dir`` or ``PYJAVA_SPOOL_DIR`` is the parent when set.
    The actor creates the directory. Failure cleanup removes only this path.
    """
    parent = None
    if conf:
        parent = conf.get("python.socket.shared.dir") or None
    if not parent:
        parent = os.environ.get("PYJAVA_SPOOL_DIR") or tempfile.gettempdir()
    safe = str(worker_id).replace("/", "_").replace(os.sep, "_")
    if safe in ("", ".", ".."):
        raise ValueError("snapshot actor id is empty")
    return os.path.join(parent, safe)


def identity_value(value, default=-1):
    """Keep numeric identity, including zero. Missing stays ``default``.

    ``int(value or -1)`` is wrong: partition 0 and attempt 0 are real ids.
    """
    if value is None:
        return default
    try:
        if value != value:  # NaN
            return default
    except TypeError:
        pass
    return int(value)


class SnapshotError(RuntimeError):
    """Client-side failure carrying the transport phase and partition identity."""

    def __init__(self, phase, message, partition_id=-1, attempt_id=-1):
        self.phase = phase
        self.partition_id = partition_id
        self.attempt_id = attempt_id
        super(SnapshotError, self).__init__(
            "shared snapshot phase=%s partition=%s attempt=%s: %s" %
            (phase, partition_id, attempt_id, message))


class SharedDataServer(object):
    """Descriptor of one token-scoped snapshot on a shared listener.

    Field names are the engine-facing contract; ``as_row`` is the exact result
    row produced by ``RayContext.setup`` under ``python.socket.transport=shared``.
    """

    def __init__(self, host, port, token, protocol=PROTOCOL, partition_id=-1,
                 attempt_id=-1, snapshot_bytes=-1, lease_deadline_ms=0, timezone=""):
        self.host = host
        self.port = int(port)
        self.token = token
        self.protocol = protocol
        self.partition_id = int(partition_id)
        self.attempt_id = int(attempt_id)
        self.snapshot_bytes = int(snapshot_bytes)
        self.lease_deadline_ms = int(lease_deadline_ms)
        self.timezone = timezone or ""

    def as_row(self):
        return {name: getattr(self, name) for name in _DESCRIPTOR_FIELDS}

    @staticmethod
    def from_row(item):
        def num(key, default):
            value = item.get(key, default)
            try:
                if value is None or value != value:  # NaN
                    return default
                return int(value)
            except (TypeError, ValueError):
                return default

        return SharedDataServer(
            host=item["host"], port=num("port", 0), token=item["token"],
            protocol=item["protocol"], partition_id=num("partition_id", -1),
            attempt_id=num("attempt_id", -1), snapshot_bytes=num("snapshot_bytes", -1),
            lease_deadline_ms=num("lease_deadline_ms", 0),
            timezone=item.get("timezone", "") or "")


def _recv_exact(conn, count):
    buf = bytearray()
    while len(buf) < count:
        chunk = conn.recv(count - len(buf))
        if not chunk:
            raise EOFError("peer closed the snapshot connection")
        buf += chunk
    return bytes(buf)


def _send_request(conn, op, token):
    data = token.encode("utf-8")
    conn.sendall(struct.pack("!iiii", MAGIC, VERSION, op, len(data)) + data)


def _send_response(conn, status, info=None):
    data = (info or "").encode("utf-8")
    conn.sendall(struct.pack("!ii", status, len(data)) + data)


def _stream_exact(stream, count):
    buf = bytearray()
    while len(buf) < count:
        chunk = stream.read(count - len(buf))
        if not chunk:
            raise EOFError("snapshot service closed mid-response")
        buf += chunk
    return bytes(buf)


def _read_response(stream):
    status, info_len = struct.unpack("!ii", _stream_exact(stream, 8))
    if info_len < 0 or info_len > _MAX_INFO:
        raise IOError("invalid snapshot info frame: %s" % info_len)
    info_raw = _stream_exact(stream, info_len) if info_len else b""
    try:
        info = json.loads(info_raw.decode("utf-8")) if info_len else {}
    except ValueError:
        info = {}
    return status, info


class BoundedSnapshotInput(io.RawIOBase):
    """Reads exactly ``length`` snapshot bytes, then reports EOF.

    A short underlying read is truncation, never a clean end.
    """

    def __init__(self, stream, length, on_end=None):
        super(BoundedSnapshotInput, self).__init__()
        self.stream = stream
        self.remaining = int(length)
        self.consumed = 0
        self._on_end = on_end

    def readable(self):
        return True

    def read(self, size=-1):
        if self.remaining <= 0:
            return b""
        if size is None or size < 0 or size > self.remaining:
            size = self.remaining
        data = _stream_exact(self.stream, size)
        self.consumed += len(data)
        self.remaining -= len(data)
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)

    def expect_end(self):
        while self.remaining > 0:
            self.read(min(self.remaining, 65536))
        if self.consumed < 0:  # unreachable; keeps the invariant explicit
            raise IOError("snapshot payload length mismatch")
        if self._on_end is not None:
            self._on_end()
            self._on_end = None


class SnapshotClient(object):
    """One serial connection to a shared snapshot listener.

    STATUS/READ/RELEASE can share this socket, in order, for different tokens.
    There is no connection pool: ``fetch_shared_arrow_batches`` opens a new
    client every call and closes it, including when the generator stops early
    or a frame is corrupt. A caller that keeps this object must finish each
    READ (``expect_end``) before the next request. A short read or a bad frame
    marks the client broken; later calls fail instead of reusing the socket.
    """

    def __init__(self, host, port, connect_timeout=10.0, io_timeout=30.0,
                 partition_id=-1, attempt_id=-1):
        import pyjava.utils as utils
        self.partition_id = partition_id
        self.attempt_id = attempt_id
        self._closed = False
        self._broken = False
        self._reading = False
        try:
            self.sock = socket.create_connection((host, int(port)),
                                                 timeout=connect_timeout)
        except OSError as e:
            raise SnapshotError("connect", "%s" % e, partition_id, attempt_id)
        buffer_size = utils.configure_transfer_socket(self.sock)
        self.sock.settimeout(io_timeout)
        self.stream = self.sock.makefile("rwb", buffer_size)

    def _ensure_open(self):
        if self._reading:
            self._broken = True
            self.close()
            raise SnapshotError(
                "read",
                "previous snapshot payload was not fully consumed; "
                "this connection is not reused",
                self.partition_id, self.attempt_id)
        if self._closed or self._broken:
            raise SnapshotError(
                "connect", "snapshot connection is closed and is not reused",
                self.partition_id, self.attempt_id)

    def _break(self, exc):
        self._broken = True
        self._reading = False
        return exc

    def _request(self, op, token):
        self._ensure_open()
        try:
            data = token.encode("utf-8")
            self.stream.write(struct.pack("!iiii", MAGIC, VERSION, op, len(data)))
            self.stream.write(data)
            self.stream.flush()
        except (IOError, EOFError, OSError) as e:
            raise self._break(e)

    def _response(self):
        try:
            return _read_response(self.stream)
        except (IOError, EOFError, OSError, struct.error) as e:
            raise self._break(e)

    def status(self, token):
        self._request(OP_STATUS, token)
        return self._response()

    def release(self, token):
        self._request(OP_RELEASE, token)
        return self._response()

    def read(self, token):
        """Return (info, BoundedSnapshotInput). Call ``expect_end`` before reuse.

        A non-ready status is a complete protocol response and leaves this
        client usable. A truncated frame does not.
        """
        self._request(OP_READ, token)
        status, info = self._response()
        if status != ST_READY:
            raise SnapshotError("read", "snapshot state is %s: %s" %
                                (_STATUS_NAMES.get(status, status),
                                 info.get("error", "")),
                                self.partition_id, self.attempt_id)
        try:
            raw = _stream_exact(self.stream, 8)
            (length,) = struct.unpack("!q", raw)
        except (IOError, EOFError, OSError, struct.error) as e:
            raise self._break(e)
        if length < 0:
            raise self._break(IOError("invalid snapshot payload length"))
        self._reading = True

        def _finished():
            self._reading = False

        return info, BoundedSnapshotInput(self.stream, length, on_end=_finished)

    def wait_ready(self, token, deadline, poll=0.1):
        """Poll STATUS on this connection until the snapshot leaves preparing."""
        while True:
            status, info = self.status(token)
            if status == ST_READY:
                return info
            if status == ST_FAILED:
                raise SnapshotError("generate", "generation failed on server: %s" %
                                    info.get("error", "unknown"),
                                    self.partition_id, self.attempt_id)
            if status == ST_EXPIRED:
                raise SnapshotError("status", "snapshot expired before it was ready",
                                    self.partition_id, self.attempt_id)
            if status == ST_UNKNOWN:
                raise SnapshotError("status", "unknown snapshot token",
                                    self.partition_id, self.attempt_id)
            if status == ST_BUSY:
                raise SnapshotError("connect", "snapshot service is busy",
                                    self.partition_id, self.attempt_id)
            if status != ST_PREPARING:
                raise SnapshotError("status", "unexpected snapshot status %s" % status,
                                    self.partition_id, self.attempt_id)
            if time.monotonic() >= deadline:
                raise SnapshotError("wait-ready",
                                    "snapshot still preparing after the ready deadline",
                                    self.partition_id, self.attempt_id)
            time.sleep(min(poll, max(0.0, deadline - time.monotonic())))

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self.stream.close()
            finally:
                self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def fetch_shared_arrow_batches(server, conf=None):
    """Yield Arrow batches from one shared snapshot.

    Opens a new connection for this call and closes it when the generator
    finishes, raises, or is closed early. This is not a connection pool;
    serial multi-token reads belong to a ``SnapshotClient`` the caller owns.
    """
    import pyarrow as pa
    from pyjava.transfer import StrictArrowInput
    connect_timeout = _conf_ms(conf, "python.connect.timeout",
                               "PYJAVA_CONNECT_TIMEOUT_SECONDS", 10.0)
    read_timeout = _conf_ms(conf, "python.socket.read.timeout",
                            "PYJAVA_SOCKET_TIMEOUT_SECONDS", 300.0)
    wait_ready = _conf_ms(conf, "python.socket.shared.wait.ready.ms",
                          "PYJAVA_WAIT_READY_SECONDS", 1800.0)
    client = SnapshotClient(server.host, server.port, connect_timeout,
                            read_timeout, server.partition_id, server.attempt_id)
    try:
        client.wait_ready(server.token, time.monotonic() + wait_ready)
        _, bounded = client.read(server.token)
        for batch in pa.ipc.open_stream(StrictArrowInput(bounded)):
            yield batch
        bounded.expect_end()
    except SnapshotError:
        raise
    except (IOError, EOFError, OSError) as e:
        raise SnapshotError("read", "%s" % e, server.partition_id,
                            server.attempt_id)
    finally:
        client.close()


class _Entry(object):
    PREPARING, READY, FAILED, EXPIRED = "preparing", "ready", "failed", "expired"

    def __init__(self, token, partition_id, attempt_id, lease_ms, prepare_ms,
                 max_bytes, activate_ms):
        self.token = token
        self.partition_id = partition_id
        self.attempt_id = attempt_id
        self.lease_ms = lease_ms
        self.activate_ms = activate_ms
        self.max_bytes = max_bytes
        self.lock = threading.Lock()
        self.state = _Entry.PREPARING
        self.path = None
        self.size = -1
        self.error = None
        self.charged = 0
        self.readers = 0
        self.delete_pending = False
        self.file_deleted = False
        self.uncharged = False
        self.writer_active = False
        self.registered_ms = int(time.time() * 1000)
        self.prepare_deadline = time.monotonic() + prepare_ms / 1000.0
        self.lease_deadline_ms = 0
        self.activate_deadline = None
        self.gone_after = float("inf")

    def begin_write(self):
        with self.lock:
            if self.state != _Entry.PREPARING:
                raise IOError("snapshot generation cancelled")
            self.writer_active = True

    def ensure_preparing(self):
        with self.lock:
            if self.state != _Entry.PREPARING:
                raise IOError("snapshot generation cancelled")
            if time.monotonic() > self.prepare_deadline:
                raise IOError("snapshot generation exceeded "
                              "python.socket.shared.prepare.timeout.ms")

    def charge_write(self, count, service):
        """Account ``count`` bytes or refuse. Does not wait for a reader."""
        with self.lock:
            if self.state != _Entry.PREPARING:
                raise IOError("snapshot generation cancelled")
            if time.monotonic() > self.prepare_deadline:
                raise IOError("snapshot generation exceeded "
                              "python.socket.shared.prepare.timeout.ms")
            if self.charged + count > self.max_bytes:
                raise IOError("Partition exceeds "
                              "python.socket.shared.partition.maxBytes")
            service._charge(count)
            self.charged += count

    def ready(self, path, size, start_lease=True):
        with self.lock:
            self.writer_active = False
            self.path = path
            if self.state != _Entry.PREPARING:
                raise IOError("snapshot generation cancelled")
            self.size = size
            self.state = _Entry.READY
            if start_lease:
                self.lease_deadline_ms = int(time.time() * 1000) + self.lease_ms
                self.activate_deadline = None
            else:
                # Readable, but the lease clock starts at activate() so a
                # later partition in the same setup cannot expire this one.
                self.lease_deadline_ms = 0
                self.activate_deadline = time.monotonic() + self.activate_ms / 1000.0

    def activate(self):
        with self.lock:
            if self.state != _Entry.READY:
                raise IOError("snapshot is %s" % self.state)
            self.lease_deadline_ms = int(time.time() * 1000) + self.lease_ms
            self.activate_deadline = None
            return self.lease_deadline_ms

    def extend_activation(self, extra_ms):
        with self.lock:
            if self.state == _Entry.READY and not self.lease_deadline_ms:
                self.activate_deadline = time.monotonic() + extra_ms / 1000.0

    def fail(self, error):
        with self.lock:
            if self.state in (_Entry.PREPARING, _Entry.READY):
                self.state = _Entry.FAILED
                self.error = (error or "unknown")[:2048]
                if not self.lease_deadline_ms:
                    self.lease_deadline_ms = int(time.time() * 1000) + self.lease_ms
                if not self.writer_active:
                    self._delete_locked()

    def expire(self, grace_ms):
        with self.lock:
            if self.state != _Entry.EXPIRED:
                self.state = _Entry.EXPIRED
                self.gone_after = time.monotonic() + grace_ms / 1000.0
                if not self.writer_active:
                    self._delete_locked()

    def force_unlink(self):
        """Drop the directory entry even if the writer thread is still blocked.

        This does not interrupt that thread. On POSIX the name disappears now;
        an open fd keeps the inode until the process closes it. An error
        already stored for this attempt is left as-is.
        """
        with self.lock:
            if self.path and not self.file_deleted:
                try:
                    os.unlink(self.path)
                except FileNotFoundError:
                    self.file_deleted = True
                except OSError:
                    return False
                else:
                    self.file_deleted = True
            if self.file_deleted or not self.path:
                self.writer_active = False
            if self.state in (_Entry.PREPARING, _Entry.READY):
                self.state = _Entry.FAILED
                if not self.error:
                    self.error = "snapshot generation cancelled"
                if not self.lease_deadline_ms:
                    self.lease_deadline_ms = int(time.time() * 1000) + self.lease_ms
            return True

    def abort_write(self, error):
        """Writer is leaving. Delete now unless a reader still holds the file."""
        with self.lock:
            self.writer_active = False
            if self.state == _Entry.PREPARING:
                self.state = _Entry.FAILED
                self.error = (error or "unknown")[:2048]
                if not self.lease_deadline_ms:
                    self.lease_deadline_ms = int(time.time() * 1000) + self.lease_ms
            self._delete_locked()

    def _delete_locked(self):
        if self.path and not self.file_deleted:
            if self.readers > 0:
                self.delete_pending = True
            else:
                try:
                    os.unlink(self.path)
                except OSError:
                    pass
                self.file_deleted = True

    def release_charge(self):
        """Return bytes to subtract, once, after the file is actually gone.

        Active writers and readers keep the charge so a release cannot free
        budget for bytes that are still on disk.
        """
        with self.lock:
            if self.uncharged or self.writer_active:
                return 0
            if self.readers > 0:
                return 0
            if self.path and not self.file_deleted:
                return 0
            self.uncharged = True
            amount = self.charged
            self.charged = 0
            return amount

    def open_reader(self):
        with self.lock:
            if self.state != _Entry.READY or not self.path or self.file_deleted:
                return None
            self.readers += 1
            try:
                return open(self.path, "rb")
            except BaseException:
                self.readers -= 1
                raise

    def close_reader(self):
        with self.lock:
            if self.readers > 0:
                self.readers -= 1
            if self.delete_pending and self.readers <= 0 and not self.file_deleted:
                try:
                    os.unlink(self.path)
                except OSError:
                    pass
                self.file_deleted = True

    def retained(self):
        with self.lock:
            if self.writer_active or self.readers > 0:
                return True
            return self.state != _Entry.EXPIRED

    def status_code(self):
        with self.lock:
            return {p: s for p, s in
                    [(_Entry.READY, ST_READY), (_Entry.PREPARING, ST_PREPARING),
                     (_Entry.FAILED, ST_FAILED), (_Entry.EXPIRED, ST_EXPIRED)]
                    }[self.state]

    def info(self):
        with self.lock:
            payload = {"state": self.state, "partitionId": self.partition_id,
                       "attemptId": self.attempt_id, "snapshotBytes": self.size,
                       "leaseDeadlineMs": self.lease_deadline_ms}
            if self.error:
                payload["error"] = self.error
            return json.dumps(payload, ensure_ascii=False)


class _ChargingWriter(object):
    """File-like writer. Each write checks cancel/deadline and charges budget.

    The charge stays until the file is deleted and the last reader is gone.
    """

    def __init__(self, service, entry, raw):
        self.service = service
        self.entry = entry
        self.raw = raw
        self.written = 0

    def write(self, data):
        count = len(data)
        if count == 0:
            return 0
        self.entry.charge_write(count, self.service)
        try:
            written = self.raw.write(data)
        except BaseException:
            # Bytes were charged and may not be in the file. The writer's
            # abort settles the entry; keep ``written`` honest for the size.
            self.written += count
            raise
        self.written += count
        return written

    def __getattr__(self, name):
        return getattr(self.raw, name)


class SnapshotService(object):
    """Process-level listener serving many token-scoped Arrow snapshots."""

    def __init__(self, host, port=0, conf=None):
        self.conf = dict(conf or {})
        self.host = host
        self.max_snapshots = _conf_int(self.conf, "python.socket.shared.maxSnapshots",
                                       "PYJAVA_SNAPSHOT_MAX_SNAPSHOTS", 256)
        self.total_max = _conf_int(self.conf, "python.socket.shared.total.maxBytes",
                                   "PYJAVA_SNAPSHOT_TOTAL_MAX_BYTES", 4 * 1024 ** 3)
        self.max_connections = _conf_int(self.conf, "python.socket.shared.maxConnections",
                                         "PYJAVA_SNAPSHOT_MAX_CONNECTIONS", 64)
        self.handshake_timeout = _conf_ms(self.conf, "python.socket.shared.handshake.timeout",
                                          "PYJAVA_SNAPSHOT_HANDSHAKE_TIMEOUT", 10.0)
        self.io_timeout = _conf_ms(self.conf, "python.socket.write.timeout",
                                   "PYJAVA_SOCKET_TIMEOUT_SECONDS", 300.0)
        self.lease_ms = _conf_int(self.conf, "python.socket.shared.lease.ms",
                                  "PYJAVA_SNAPSHOT_LEASE_MS", 30 * 60 * 1000)
        self.prepare_ms = _conf_int(self.conf, "python.socket.shared.prepare.timeout.ms",
                                    "PYJAVA_SNAPSHOT_PREPARE_MS", self.lease_ms)
        self.grace_ms = _conf_int(self.conf, "python.socket.shared.expired.grace.ms",
                                  "PYJAVA_SNAPSHOT_EXPIRED_GRACE_MS", 60000)
        self.activate_ms = _conf_int(
            self.conf, "python.socket.shared.activate.timeout.ms",
            "PYJAVA_SNAPSHOT_ACTIVATE_MS", self.lease_ms)
        self.partition_max = _conf_int(
            self.conf, "python.socket.shared.partition.maxBytes",
            "PYJAVA_SPOOL_MAX_BYTES",
            int(_conf_number(self.conf, "python.socket.spool.maxBytes",
                             "PYJAVA_SPOOL_MAX_BYTES", 1024 ** 3)))
        # A private subdirectory, never the caller's parent. close() removes
        # only this directory, including a file a blocked writer still has open.
        parent = self.conf.get("python.socket.shared.dir") or \
            os.environ.get("PYJAVA_SPOOL_DIR")
        if parent:
            os.makedirs(parent, exist_ok=True)
            self._owned_dir = tempfile.mkdtemp(prefix="pyjava-snapshot-", dir=parent)
        else:
            self._owned_dir = tempfile.mkdtemp(prefix="pyjava-snapshot-")
        self.spool_dir = self._owned_dir
        self.inflight_limit = _conf_int(
            self.conf, "python.ray.inflight.generations",
            "PYJAVA_INFLIGHT_GENERATIONS", 2)
        # Safety net inside one process. The job-wide cap is the Ray scheduler.
        # acquire is non-blocking: exceeding the cap fails instead of waiting
        # for a future reader to free a slot.
        self.generation_permits = threading.BoundedSemaphore(self.inflight_limit)
        self.budget_signature = {
            "python.socket.shared.total.maxBytes": int(self.total_max),
            "python.socket.shared.maxSnapshots": int(self.max_snapshots),
            "python.socket.shared.maxConnections": int(self.max_connections),
        }
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener.bind((host, int(port)))
            self.listener.listen(64)
        except BaseException:
            try:
                self.listener.close()
            except OSError:
                pass
            self._remove_owned_dir()
            raise
        self.listener.settimeout(0.5)
        self.port = self.listener.getsockname()[1]
        self.entries = {}
        self.total_bytes = 0
        self._lock = threading.Lock()
        self._conn_slots = threading.BoundedSemaphore(self.max_connections)
        self._conns = set()
        self._conns_lock = threading.Lock()
        self._closed = threading.Event()
        self._acceptor = threading.Thread(target=self._accept_loop, daemon=True,
                                          name="pyjava-snapshot-accept")
        self._reaper = threading.Thread(target=self._reap_loop, daemon=True,
                                        name="pyjava-snapshot-reaper")
        self._acceptor.start()
        self._reaper.start()

    # ---- quota -----------------------------------------------------------
    def _charge(self, count):
        with self._lock:
            if self.total_bytes + count > self.total_max:
                raise IOError("Process snapshot byte budget exceeded "
                              "(python.socket.shared.total.maxBytes)")
            self.total_bytes += count

    def _uncharge(self, count):
        if not count:
            return
        if count < 0:
            raise ValueError("negative snapshot uncharge")
        with self._lock:
            if self.total_bytes < count:
                raise ValueError(
                    "snapshot budget would become negative (%s - %s)" %
                    (self.total_bytes, count))
            self.total_bytes -= count

    def _settle(self, entry):
        """Drop budget only for bytes whose file is already gone."""
        self._uncharge(entry.release_charge())

    def assert_compatible(self, conf):
        """Service budgets are fixed by the first open. A different value fails.

        Omitting a key is not a conflict; the already-open service keeps the
        value from its first configuration. Per-token lease and partition caps
        are not process budgets and may differ per register call.
        """
        conf = conf or {}
        for key, effective in self.budget_signature.items():
            raw = conf.get(key)
            if raw is None:
                continue
            requested = int(raw)
            if requested != int(effective):
                raise ValueError(
                    "shared snapshot service %s on %s:%s was already configured "
                    "with %s=%s; refusing conflicting %s=%s. These budgets are "
                    "process-level and fixed by the first request. Use a "
                    "different python.socket.shared.name or the same values." %
                    (self.conf.get("python.socket.shared.name", "default"),
                     self.host, self.port, key, effective, key, requested))

    # ---- producer API ------------------------------------------------------
    def register(self, partition_id=-1, attempt_id=-1, lease_ms=None,
                 prepare_ms=None, activate_ms=None):
        """Reserve a token for a new snapshot attempt; raises on registry limit."""
        if self._closed.is_set():
            raise IOError("shared snapshot service is closed")
        with self._lock:
            if len(self.entries) >= self.max_snapshots:
                raise IOError("shared snapshot registry is full "
                              "(python.socket.shared.maxSnapshots=%s)" %
                              self.max_snapshots)
            token = secrets.token_hex(16)
            entry = _Entry(token, identity_value(partition_id, -1),
                           identity_value(attempt_id, -1),
                           self.lease_ms if lease_ms is None else int(lease_ms),
                           self.prepare_ms if prepare_ms is None else int(prepare_ms),
                           self.partition_max,
                           self.activate_ms if activate_ms is None else int(activate_ms))
            if entry.lease_ms <= 0 or entry.activate_ms <= 0:
                raise ValueError("snapshot lease and activation deadlines must be positive")
            self.entries[token] = entry
            return token

    def materialize(self, token, data, schema=None, cancelled=lambda: False,
                    start_lease=True):
        """Commit Arrow batches to a bounded temp file, then mark the token ready.

        The in-flight permit is held only while writing, never until a consumer
        reads the snapshot. If every permit is already taken, this fails at
        once instead of waiting for a reader. ``start_lease=False`` leaves the
        snapshot ready with no lease clock until ``activate``.
        """
        from pyjava.transfer import arrow_batches, write_batches
        entry = self.entries[token]
        if not self.generation_permits.acquire(blocking=False):
            entry.fail("in-flight snapshot generations exceeded "
                       "python.ray.inflight.generations=%s" % self.inflight_limit)
            self._settle(entry)
            raise IOError("in-flight snapshot generations exceeded "
                          "python.ray.inflight.generations=%s" % self.inflight_limit)
        path = None
        try:
            entry.begin_write()
            fd, path = tempfile.mkstemp(prefix="pyjava-snapshot-", suffix=".arrow",
                                        dir=self.spool_dir)
            entry.path = path
            with os.fdopen(fd, "wb") as raw:
                writer = _ChargingWriter(self, entry, raw)
                import pyarrow as pa
                if isinstance(data, pa.Table):
                    source = data.to_batches()
                elif isinstance(data, pa.RecordBatch):
                    source = (data,)
                else:
                    source = data

                def guarded():
                    for item in source:
                        entry.ensure_preparing()
                        if cancelled() or self._closed.is_set():
                            raise InterruptedError("Snapshot materialization cancelled")
                        yield item

                def checked():
                    for batch in arrow_batches(guarded(), schema):
                        entry.ensure_preparing()
                        if cancelled() or self._closed.is_set():
                            raise InterruptedError("Snapshot materialization cancelled")
                        yield batch

                write_batches(checked(), writer)
                writer.flush()
            size = writer.written
            entry.ready(path, size, start_lease=start_lease)
            return size
        except BaseException as e:
            if path is not None:
                entry.path = path
            entry.abort_write("%s: %s" % (type(e).__name__, str(e)[:400]))
            self._settle(entry)
            raise
        finally:
            self.generation_permits.release()

    def activate(self, token):
        return self.entries[token].activate()

    def keepalive_deferred(self):
        """Push back activation deadlines for snapshots whose lease has not started."""
        for entry in list(self.entries.values()):
            entry.extend_activation(self.activate_ms)

    def fail(self, token, error):
        entry = self.entries.get(token)
        if entry is None:
            return
        entry.fail(error)
        self._settle(entry)

    def release(self, token):
        entry = self.entries.get(token)
        if entry is None:
            return False
        entry.expire(self.grace_ms)
        self._settle(entry)
        return True

    def retained_count(self):
        return sum(1 for entry in list(self.entries.values()) if entry.retained())

    def status(self, token):
        entry = self.entries.get(token)
        if entry is None:
            return {"state": "unknown"}
        return json.loads(entry.info())

    # ---- listener ----------------------------------------------------------
    def _accept_loop(self):
        while not self._closed.is_set():
            try:
                conn, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            if not self._conn_slots.acquire(blocking=False):
                try:
                    _send_response(conn, ST_BUSY, "connection limit")
                except OSError:
                    pass
                finally:
                    conn.close()
                continue
            with self._conns_lock:
                self._conns.add(conn)
            threading.Thread(target=self._guarded_handle, args=(conn,),
                             daemon=True, name="pyjava-snapshot-conn").start()

    def _guarded_handle(self, conn):
        try:
            self._handle(conn)
        except (EOFError, socket.timeout, OSError):
            pass
        except BaseException:
            logging.exception("shared snapshot connection failed")
        finally:
            with self._conns_lock:
                self._conns.discard(conn)
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            finally:
                self._conn_slots.release()

    def _handle(self, conn):
        while not self._closed.is_set():
            conn.settimeout(self.handshake_timeout)
            try:
                magic, version, op = struct.unpack("!iii", _recv_exact(conn, 12))
            except EOFError:
                return
            if magic != MAGIC or version != VERSION:
                return
            (token_len,) = struct.unpack("!i", _recv_exact(conn, 4))
            if token_len <= 0 or token_len > _MAX_TOKEN:
                return
            token = _recv_exact(conn, token_len).decode("utf-8")
            entry = self.entries.get(token)
            if op == OP_STATUS:
                if entry is None:
                    _send_response(conn, ST_UNKNOWN)
                else:
                    _send_response(conn, entry.status_code(), entry.info())
            elif op == OP_RELEASE:
                if entry is None:
                    _send_response(conn, ST_UNKNOWN)
                else:
                    self.release(token)
                    _send_response(conn, ST_READY)
            elif op == OP_READ:
                if entry is None:
                    _send_response(conn, ST_UNKNOWN)
                    continue
                code = entry.status_code()
                if code != ST_READY:
                    _send_response(conn, code, entry.info())
                    continue
                reader = entry.open_reader()
                if reader is None:
                    _send_response(conn, entry.status_code(), entry.info())
                    continue
                try:
                    _send_response(conn, ST_READY, entry.info())
                    conn.sendall(struct.pack("!q", entry.size))
                    conn.settimeout(self.io_timeout)
                    with reader:
                        while True:
                            chunk = reader.read(1024 * 1024)
                            if not chunk:
                                break
                            conn.sendall(chunk)
                finally:
                    entry.close_reader()
                    self._settle(entry)
            else:
                try:
                    _send_response(conn, ST_BAD)
                finally:
                    return

    def _reap_loop(self):
        while not self._closed.wait(0.5):
            now = time.monotonic()
            now_ms = time.time() * 1000.0
            for token, entry in list(self.entries.items()):
                state = entry.state
                if state == _Entry.PREPARING and now > entry.prepare_deadline:
                    entry.fail("snapshot generation exceeded "
                               "python.socket.shared.prepare.timeout.ms")
                    self._settle(entry)
                    state = entry.state
                if state == _Entry.READY and not entry.lease_deadline_ms and \
                        entry.activate_deadline is not None and \
                        now > entry.activate_deadline:
                    entry.fail("snapshot was not activated before "
                               "python.socket.shared.activate.timeout.ms")
                    self._settle(entry)
                    state = entry.state
                if state in (_Entry.READY, _Entry.FAILED) and \
                        entry.lease_deadline_ms and now_ms > entry.lease_deadline_ms:
                    entry.expire(self.grace_ms)
                    self._settle(entry)
                if entry.state == _Entry.EXPIRED and now > entry.gone_after:
                    with self._lock:
                        self.entries.pop(token, None)
                    self._settle(entry)

    # ---- shutdown -----------------------------------------------------------
    def close(self):
        first = not self._closed.is_set()
        self._closed.set()
        if first:
            try:
                self.listener.close()
            except OSError:
                pass
            with self._conns_lock:
                conns = list(self._conns)
            for conn in conns:
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    conn.close()
                except OSError:
                    pass
            # Unlink even when the writer thread is blocked in user code.
            # Expire would leave that file behind because writer_active is set.
            for token in list(self.entries):
                entry = self.entries.get(token)
                if entry is not None:
                    entry.force_unlink()
                    self._settle(entry)
            self._acceptor.join(2)
            self._reaper.join(2)
        self._remove_owned_dir()

    def _remove_owned_dir(self):
        path = getattr(self, "_owned_dir", None)
        if path:
            shutil.rmtree(path, ignore_errors=True)


_services = {}
_services_lock = threading.Lock()


def get_shared_service(host, port=0, conf=None):
    """One shared listener per (name, host, requested port) inside this process.

    The first configuration fixes ``total.maxBytes``, ``maxSnapshots`` and
    ``maxConnections``. A later request that names a different value for one
    of those keys is rejected. It does not silently keep the old budget.
    """
    conf = dict(conf or {})
    key = (conf.get("python.socket.shared.name", "default"), host, int(port))
    with _services_lock:
        service = _services.get(key)
        if service is None or service._closed.is_set():
            service = SnapshotService(host, port, conf)
            _services[key] = service
            return service
        service.assert_compatible(conf)
        return service


def shutdown_shared_services():
    with _services_lock:
        services = list(_services.values())
        _services.clear()
    for service in services:
        service.close()
