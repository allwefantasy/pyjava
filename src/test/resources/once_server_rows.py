"""Ray's OnceServer, as SparkSocketRunner.readFromStreamWithArrow sees it."""
import struct
import sys

from pyjava.api.serve import OnceServer

count = int(sys.argv[1])
server = OnceServer("127.0.0.1", 0, "UTC")
_host, port = server.bind()
if not server.is_bind:
    raise SystemExit("OnceServer failed to bind")
sys.stdout.buffer.write(struct.pack("!i", port))
sys.stdout.buffer.flush()
server.serve({"value": i} for i in range(count))
