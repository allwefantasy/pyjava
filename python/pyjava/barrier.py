"""Callback from a Python worker to the JVM barrier socket.

The data socket stays dedicated to Arrow. A barrier task opens a second
loopback socket and puts its port in the task header. This client speaks
PyJava's framing, not PySpark's authenticated multi-message framing:

  request:  one big-endian int, BARRIER_FUNCTION (1)
  response: one UTF-8 string, "success" or the JVM exception text
"""

import time

from pyjava.serializers import UTF8Deserializer, write_int
from pyjava.utils import local_connect_and_auth

BARRIER_FUNCTION = 1
BARRIER_RESULT_SUCCESS = "success"


class BarrierTaskContext(object):
    """Per-worker view of the current task. The port is replaced on every request."""

    _instance = None
    _enabled = False
    _port = None

    @classmethod
    def initialize(cls, enabled, port):
        port = int(port or 0)
        cls._enabled = bool(enabled) and port > 0
        cls._port = port if cls._enabled else None
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        return cls._instance

    @classmethod
    def reset(cls):
        cls._enabled = False
        cls._port = None

    @classmethod
    def get(cls):
        if not cls._enabled or cls._instance is None:
            raise RuntimeError("It is not in a barrier stage")
        return cls._instance

    def barrier(self):
        """Block until every task in this barrier stage reaches this call."""
        if not type(self)._enabled or not type(self)._port:
            raise RuntimeError("It is not in a barrier stage")
        _call_java_barrier(type(self)._port)

    def allGather(self, message=""):
        raise RuntimeError(
            "PyJava only forwards barrier() to the JVM task. allGather() is not available.")

    def getTaskInfos(self):
        raise RuntimeError(
            "PyJava only forwards barrier() to the JVM task. getTaskInfos() is not available.")


def _call_java_barrier(port):
    last_error = None
    sockfile = None
    sock = None
    for attempt in range(3):
        try:
            sockfile, sock = local_connect_and_auth(int(port))
            last_error = None
            break
        except Exception as error:
            last_error = error
            sockfile = None
            sock = None
            time.sleep(0.05 * (attempt + 1))
    if sockfile is None:
        raise RuntimeError(
            "Cannot connect to the Java barrier callback on port %s: %s" % (port, last_error))
    try:
        write_int(BARRIER_FUNCTION, sockfile)
        sockfile.flush()
        try:
            result = UTF8Deserializer().loads(sockfile)
        except EOFError:
            raise RuntimeError("Java barrier callback closed the connection")
        if result != BARRIER_RESULT_SUCCESS:
            raise RuntimeError(result or "Java barrier call failed")
    finally:
        try:
            sockfile.close()
        finally:
            sock.close()
