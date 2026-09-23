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
