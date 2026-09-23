"""Spark task <-> Ray OnceServer data exchange, without starting Ray."""
import socket
import struct
import threading
import unittest

import pyarrow as pa

from pyjava.api.serve import OnceServer
from pyjava.serializers import SpecialLengths


def _read_rows(host, port):
    sock = socket.create_connection((host, port), timeout=10)
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        stream = sock.makefile("rwb", 1024 * 1024)
        try:
            start = struct.unpack("!i", stream.read(4))[0]
            if start != SpecialLengths.START_ARROW_STREAM:
                raise AssertionError("missing arrow start marker: %s" % start)
            table = pa.ipc.open_stream(stream).read_all()
            end, eos = struct.unpack("!ii", stream.read(8))
            if (end, eos) != (SpecialLengths.END_OF_DATA_SECTION, SpecialLengths.END_OF_STREAM):
                raise AssertionError("bad end markers: %s %s" % (end, eos))
            stream.write(struct.pack("!i", SpecialLengths.END_OF_STREAM))
            stream.flush()
            return table.column(0).to_pylist()
        finally:
            stream.close()
    finally:
        sock.close()


class OnceServerExchangeTests(unittest.TestCase):
    def test_once_server_round_trip_uses_the_spark_ack(self):
        server = OnceServer("127.0.0.1", 0, "UTC")
        host, port = server.bind()
        self.assertTrue(server.is_bind)
        rows = [{"value": i} for i in range(1000)]
        thread = threading.Thread(target=server.serve, args=(rows,))
        thread.start()
        try:
            self.assertEqual(_read_rows(host, port), list(range(1000)))
        finally:
            thread.join(10)
            server.close()
        self.assertFalse(thread.is_alive())

    def test_concurrent_once_servers_do_not_mix_partitions(self):
        servers = []
        threads = []
        expected = []
        for shard in range(4):
            server = OnceServer("127.0.0.1", 0, "UTC")
            host, port = server.bind()
            values = [{"value": shard * 1000 + i} for i in range(500)]
            thread = threading.Thread(target=server.serve, args=(values,))
            thread.start()
            servers.append((server, host, port))
            threads.append(thread)
            expected.append([row["value"] for row in values])
        try:
            actual = [_read_rows(host, port) for _, host, port in servers]
        finally:
            for thread in threads:
                thread.join(10)
            for server, _, _ in servers:
                server.close()
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
