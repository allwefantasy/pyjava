import os
import socket

from pyjava.serializers import ArrowStreamPandasSerializer, read_int, write_int

out_ser = ArrowStreamPandasSerializer("Asia/Harbin", False, None)
HOST = "127.0.0.1"
PORT = 11111
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((HOST, PORT))
    buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
    infile = os.fdopen(os.dup(sock.fileno()), "rb", buffer_size)
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", buffer_size)
    # arrow start
    print(read_int(infile))
    kk = out_ser.load_stream(infile)

    for item in kk:
        print(item)
    # end data
    print(read_int(infile))
    # end stream
    print(read_int(infile))
    write_int(-4,outfile)
