import io
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import pyarrow as pa
from pyjava.api.mlsql import DataServer, RayContext
from pyjava.api.serve import OnceServer
from pyjava.transfer import ArrowSpool, StrictArrowInput, arrow_batches


class TransferTests(unittest.TestCase):
    def test_nullable_int64_rows_are_exact_over_tcp(self):
        value = 2**53 + 1
        batch = pa.record_batch([pa.array([value, None], type=pa.int64())], names=["id"])
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, batch.schema) as writer:
            writer.write_batch(batch)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        def send():
            conn, _ = listener.accept()
            with conn:
                conn.sendall(buf.getvalue())
        t = threading.Thread(target=send)
        t.start()
        try:
            rows = list(RayContext.fetch_once_as_rows(DataServer("127.0.0.1", listener.getsockname()[1], "UTC")))
            self.assertEqual(rows, [{"id": value}, {"id": None}])
        finally:
            t.join(2)
            listener.close()

    def test_truncation_at_batch_boundary_is_an_error(self):
        buf = io.BytesIO()
        schema = pa.schema([("id", pa.int64())])
        with pa.ipc.new_stream(buf, schema) as writer:
            writer.write_batch(pa.record_batch([[1, 2]], schema=schema))
        for cut in (8, 9, 10):
            with self.assertRaises((EOFError, pa.ArrowInvalid, OSError)):
                list(pa.ipc.open_stream(StrictArrowInput(io.BytesIO(buf.getvalue()[:-cut]))))

    def test_batches_bound_bytes_preserve_types_and_empty_schema(self):
        with patch.dict(os.environ, {"PYJAVA_ARROW_MAX_BYTES_PER_BATCH": "4096"}):
            batches = list(arrow_batches(({"id": 2**53+1, "body": "文" * 100} for _ in range(100))))
            self.assertEqual(sum(b.num_rows for b in batches), 100)
            self.assertTrue(all(b.nbytes <= 4096 for b in batches))
            self.assertTrue(all(r["id"] == 2**53+1 for b in batches for r in b.to_pylist()))
            with self.assertRaises(ValueError):
                list(arrow_batches([{"body": "x" * 8192}]))
        schema = pa.schema([("id", pa.int64()), ("body", pa.binary())])
        self.assertEqual(list(arrow_batches([], schema))[0].schema, schema)

    def test_large_native_batch_and_row_schema_changes(self):
        # Row-limit splitting must not recurse once for every output batch.
        with patch.dict(os.environ, {"PYJAVA_ARROW_MAX_RECORDS_PER_BATCH": "1"}):
            batch = pa.record_batch([range(2500)], names=["id"])
            self.assertEqual(sum(b.num_rows for b in arrow_batches(batch)), 2500)
        for rows in ([{"id": 1}, {"id": 2, "extra": 3}],
                     [{"id": 1, "extra": 3}, {"id": 2}]):
            # Detect this even before the first batch infers its Arrow schema.
            with self.assertRaisesRegex(ValueError, "Row fields changed"):
                list(arrow_batches(rows))

    def _start(self, replay=False):
        server = OnceServer("127.0.0.1", 0, "UTC")
        address = server.bind()
        errors, produced = [], []
        def data():
            for i in range(32):
                produced.append(i)
                yield {"id": i}
        def run():
            try:
                (server.serve_replayable if replay else server.serve)(data())
            except Exception as e:
                errors.append(e)
        t = threading.Thread(target=run)
        t.start()
        self.addCleanup(lambda: t.join(3))
        self.addCleanup(server.close)
        return server, address, t, errors, produced

    def _consume(self, address, ack=-4):
        with socket.create_connection(address, timeout=3) as sock:
            with sock.makefile("rwb") as stream:
                self.assertEqual(struct.unpack("!i", stream.read(4))[0], -6)
                rows = pa.ipc.open_stream(StrictArrowInput(stream)).read_all().to_pylist()
                self.assertEqual(struct.unpack("!ii", stream.read(8)), (-1, -4))
                if ack is not None:
                    stream.write(struct.pack("!i", ack))
                    stream.flush()
                return rows

    def test_wrong_ack_fails_and_close_unblocks_ack_wait(self):
        server, address, t, errors, _ = self._start()
        self._consume(address, 12345)
        t.join(2)
        self.assertFalse(t.is_alive())
        self.assertIsInstance(errors[0], IOError)
        server, address, t, errors, _ = self._start()
        with socket.create_connection(address, timeout=3) as sock:
            with sock.makefile("rwb") as stream:
                stream.read(4)
                pa.ipc.open_stream(stream).read_all()
                stream.read(8)
                server.close()
                t.join(2)
                self.assertFalse(t.is_alive())

    def test_replay_does_not_rerun_transformation_after_failed_ack(self):
        server, address, t, errors, produced = self._start(replay=True)
        first = self._consume(address, None)
        second = self._consume(address)
        self.assertEqual(first, second)
        self.assertEqual(produced, list(range(32)))
        server.close()
        t.join(2)
        self.assertFalse(t.is_alive())

    def test_spool_quota_and_cancel(self):
        with patch.dict(os.environ, {"PYJAVA_SPOOL_MAX_BYTES": "4096"}):
            with self.assertRaises(IOError):
                ArrowSpool([{"data": "x" * 4096}])
        with self.assertRaises(InterruptedError):
            ArrowSpool([{"id": 1}], cancelled=lambda: True)

    def test_module_exit_is_nonzero_when_a_scenario_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, PYJAVA_ARROW_MAX_RECORDS_PER_BATCH="0")
            result = subprocess.run([sys.executable, "-m", "pyjava.loadtest", "--scenario", "spark-ray",
                "--workers", "1", "--requests", "1", "--rows", "1", "--output", directory + "/report.json"],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
            self.assertEqual(result.returncode, 1, result.stdout.decode())


if __name__ == "__main__":
    unittest.main()
