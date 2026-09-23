"""Small TCP fault fixture for JVM acquisition tests (stdlib only)."""
import os
import socket
import struct
import sys
import threading
import time

mode = os.environ.get("PYJAVA_FAULT", "echo")
if mode == "silent":
    time.sleep(60)
    sys.exit(0)

listener = socket.socket()
listener.bind(("127.0.0.1", 0))
listener.listen()
sys.stdout.buffer.write(struct.pack("!i", listener.getsockname()[1]))
sys.stdout.buffer.flush()


def serve(conn):
    with conn:
        if mode == "handshake_timeout":
            time.sleep(10)
            return
        if mode == "negative_pid":
            conn.sendall(struct.pack("!i", -11))
            return
        conn.sendall(struct.pack("!i", os.getpid()))
        while True:
            data = conn.recv(4096)
            if not data:
                return
            conn.sendall(data)


while True:
    conn, _ = listener.accept()
    threading.Thread(target=serve, args=(conn,), daemon=True).start()
