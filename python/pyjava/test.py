import os
import socket

from pyjava.serializers import \
    ArrowStreamPandasSerializer

out_ser = ArrowStreamPandasSerializer(None, True, True)

out_ser = ArrowStreamPandasSerializer("Asia/Harbin", False, None)
HOST = "192.168.216.157"
PORT = 57979
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((HOST, PORT))
    buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
    infile = os.fdopen(os.dup(sock.fileno()), "rb", buffer_size)
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", buffer_size)
    kk = out_ser.load_stream(infile)
    
    for item in kk:
        print(item)

import pandas as pd
lst = ['Geeks', 'For', 'Geeks', 'is',
       'portal', 'for', 'Geeks']

# Calling DataFrame constructor on list
df = pd.DataFrame(lst)
df.to_json()
import ray
ray.remote()