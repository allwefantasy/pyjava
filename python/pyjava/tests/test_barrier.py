"""Barrier callback client, without a daemon or a JVM."""
import socket
import struct
import threading
import unittest

from pyjava.barrier import BarrierTaskContext


def _serve(reply):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    port = listener.getsockname()[1]
    seen = []

    def loop():
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                break
            with conn:
                stream = conn.makefile("rwb")
                seen.append(struct.unpack("!i", stream.read(4))[0])
                message = reply() if callable(reply) else reply
                data = message.encode("utf-8")
                stream.write(struct.pack("!i", len(data)))
                stream.write(data)
                stream.flush()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return listener, port, seen


class BarrierClientTests(unittest.TestCase):
    def tearDown(self):
        BarrierTaskContext.reset()

    def test_barrier_is_rejected_before_a_stage_is_initialized(self):
        with self.assertRaisesRegex(RuntimeError, "barrier stage"):
            BarrierTaskContext.get().barrier()

    def test_two_calls_and_then_a_java_error(self):
        answers = iter(("success", "success", "barrier timed out"))
        listener, port, seen = _serve(lambda: next(answers))
        try:
            BarrierTaskContext.initialize(True, port)
            BarrierTaskContext.get().barrier()
            BarrierTaskContext.get().barrier()
            with self.assertRaisesRegex(RuntimeError, "barrier timed out"):
                BarrierTaskContext.get().barrier()
            self.assertEqual(seen, [1, 1, 1])
        finally:
            listener.close()

    def test_all_gather_and_task_infos_are_explicitly_unavailable(self):
        BarrierTaskContext.initialize(True, 1)
        context = BarrierTaskContext.get()
        with self.assertRaisesRegex(RuntimeError, "allGather"):
            context.allGather("hello")
        with self.assertRaisesRegex(RuntimeError, "getTaskInfos"):
            context.getTaskInfos()

    def test_closed_port_fails_instead_of_hanging(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.close()
        BarrierTaskContext.initialize(True, port)
        with self.assertRaisesRegex(RuntimeError, "Cannot connect"):
            BarrierTaskContext.get().barrier()

    def test_disabling_the_stage_drops_the_previous_port(self):
        BarrierTaskContext.initialize(True, 9)
        BarrierTaskContext.initialize(False, 0)
        with self.assertRaisesRegex(RuntimeError, "barrier stage"):
            BarrierTaskContext.get().barrier()


if __name__ == "__main__":
    unittest.main()
