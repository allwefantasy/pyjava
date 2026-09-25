"""Shared snapshot service as SparkSocketRunner.readFromSharedSnapshot sees it.

Prints "port\ntoken" on stdout, then serves until stdin closes.
Modes: serve <count> | fail | slow <count>
"""
import sys
import threading
import time

from pyjava.snapshot import SnapshotService

mode = sys.argv[1] if len(sys.argv) > 1 else "serve"
count = int(sys.argv[2]) if len(sys.argv) > 2 else 1000

service = SnapshotService("127.0.0.1", 0, {})
token = service.register(partition_id=0, attempt_id=0)
sys.stdout.write("%d\n%s\n" % (service.port, token))
sys.stdout.flush()


def materialize():
    service.materialize(token, ({"value": i} for i in range(count)))


if mode == "serve":
    materialize()
elif mode == "fail":
    service.fail(token, "intentional generation failure")
elif mode == "slow":
    threading.Thread(target=lambda: (time.sleep(2), materialize()),
                     daemon=True).start()
else:
    raise SystemExit("unknown mode: %s" % mode)

sys.stdin.read()
service.close()
