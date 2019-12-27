from pyjava.serializers import \
    write_with_length, \
    write_int, \
    read_int, read_bool, SpecialLengths, UTF8Deserializer, \
    PickleSerializer, ArrowStreamPandasSerializer, ArrowStreamSerializer

out_ser = ArrowStreamPandasSerializer("Asia/Harbin", False, None)

ser = ArrowStreamSerializer()

# wow = open("/tmp/jjjj2", "wb")
# ddata = pd.DataFrame(data=[[1, 2, 3, 4], [2, 3, 4, 5]])
# out_ser.dump_stream([[ddata[0], ddata[1]]], wow)
# wow.close()
# wow = open("/tmp/jjjj2", "rb")
# kk = ser.load_stream(wow)
# buff = []
# for item in kk:
#     buff.append(item.to_pydict())
#
#
# def build_result(items, block_size=1024):
#     buffer = []
#     for item in items:
#         buffer.append(item)
#         if len(buffer) == block_size:
#             df = pd.DataFrame(buffer)
#             yield df
#             buffer.clear()
#     if len(buffer) > 0:
#         df = pd.DataFrame(buffer)
#         yield df
#         buffer.clear()
#
#
# items = build_result(buff, 1)
# res = ([df[name] for name in df] for df in items)
# wow3 = open("/tmp/jjjj3", "wb")
# out_ser.dump_stream(res, wow3)
# wow3.close()

wow4 = open("/tmp/jjjj-pickle", "rb")

import pickle

jack = pickle.load(wow4)
wow4.close()

for item in jack:
    print(item.to_pydict())

wow5 = open("/tmp/jjjj-pickle-wow", "wb")
ser.dump_stream(jack, wow5)
wow5.close()

wow5 = open("/tmp/jjjj-pickle-wow", "rb")
print(read_int(wow5))
wow5.close()
#
# import pyarrow as pa
# print(pa.__version__)
# wow4 = open("/tmp/jjjj-pickle", "rb")
#
# import pickle
#
# jack = pickle.load(wow4)
# wow4.close()
#print(jack[0]._to_pandas())
# wow5 = open("/tmp/jjjj-pickle-wow", "rb")
# print(read_int(wow5))
# kk = ser.load_stream(wow5)
# buff = []
# for item in kk:
#     print(item.to_pydict())




# wow = open("/tmp/jjjj", "rb")
#
# kk = out_ser.load_stream(wow)
# for item in kk:
#     print(item)

#
#
# out_ser = ArrowStreamPandasSerializer("Asia/Harbin", False, None)
# HOST = "192.168.216.157"
# PORT = 57979
# with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
#     sock.connect((HOST, PORT))
#     buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
#     infile = os.fdopen(os.dup(sock.fileno()), "rb", buffer_size)
#     outfile = os.fdopen(os.dup(sock.fileno()), "wb", buffer_size)
#     kk = out_ser.load_stream(infile)
#
#     for item in kk:
#         print(item)
#
# import pandas as pd
# lst = ['Geeks', 'For', 'Geeks', 'is',
#        'portal', 'for', 'Geeks']
