"""Read one shared snapshot from a JVM ArrowSnapshotService; print "count sum"."""
import sys
import time

import pyarrow as pa

from pyjava.snapshot import SnapshotClient

host, port, token = sys.argv[1], int(sys.argv[2]), sys.argv[3]
client = SnapshotClient(host, port, connect_timeout=10, io_timeout=60)
try:
    client.wait_ready(token, time.monotonic() + 60)
    _, stream = client.read(token)
    count = 0
    total = 0
    for batch in pa.ipc.open_stream(stream):
        values = batch.column(0).to_pylist()
        count += len(values)
        total += sum(values)
    stream.expect_end()
    sys.stdout.write("%d %d\n" % (count, total))
    sys.stdout.flush()
finally:
    client.close()
