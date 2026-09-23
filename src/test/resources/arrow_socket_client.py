"""Read one legacy Arrow stream from a SparkSocketRunner serve socket."""
import socket
import sys

import pyarrow as pa

port = int(sys.argv[1])
sock = socket.create_connection(("127.0.0.1", port))
try:
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    with sock.makefile("rb", 1024 * 1024) as stream:
        count = 0
        total = 0
        for batch in pa.ipc.open_stream(stream):
            values = batch.column(0).to_pylist()
            count += len(values)
            total += sum(values)
    sys.stdout.write("%d %d\n" % (count, total))
    sys.stdout.flush()
finally:
    sock.close()
