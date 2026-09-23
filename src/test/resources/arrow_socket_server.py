"""One-shot Arrow server used by SparkSocketRunner integration tests."""
import os
import socket
import struct
import sys

import pyarrow as pa

listener = socket.socket()
listener.bind(("127.0.0.1", 0))
listener.listen()
sys.stdout.buffer.write(struct.pack("!i", listener.getsockname()[1]))
sys.stdout.buffer.flush()
conn, _ = listener.accept()
conn.settimeout(10)
try:
    with conn.makefile("rwb") as stream:
        stream.write(struct.pack("!i", -6))
        schema = pa.schema([("value", pa.int64())])
        with pa.ipc.new_stream(stream, schema) as writer:
            for start in range(0, 100000, 1000):
                writer.write_batch(pa.record_batch([pa.array(range(start, start + 1000))], schema=schema))
        stream.write(struct.pack("!ii", -1, -123 if os.environ.get("INVALID_END") else -4))
        stream.flush()
        ack = stream.read(4)
        if ack:
            assert struct.unpack("!i", ack)[0] == -4
            assert stream.read(1) == b"", "client did not close after acknowledgement"
except (BrokenPipeError, ConnectionResetError):
    pass  # expected when the consumer stops early
finally:
    conn.close()
    listener.close()
