import os
import socket
import sys

import pandas as pd

from pyjava.serializers import ArrowStreamPandasSerializer
from pyjava.serializers import read_int
from pyjava.utils import utf8_deserializer

if sys.version >= '3':
    basestring = str
else:
    pass


class DataServer(object):
    def __init__(self, host: str, port: int, timezone: str):
        self.host = host
        self.port = port
        self.timezone = timezone


class PythonContext(object):

    def __init__(self, iterator, conf):
        self.input_data = iterator
        self.output_data = [[]]
        self.conf = conf
        self.schema = ""
        self.have_fetched = False
        if "pythonMode" in conf and conf["pythonMode"] == "ray":
            self.rayContext = RayContext(self)

    def set_output(self, value, schema=""):
        self.output_data = value
        self.schema = schema

    def _build_result(self, items, block_size=1024):
        buffer = []
        for item in items:
            buffer.append(item)
            if len(buffer) == block_size:
                df = pd.DataFrame(buffer)
                yield df
                buffer.clear()
        if len(buffer) > 0:
            df = pd.DataFrame(buffer)
            yield df
            buffer.clear()

    def build_result(self, items, block_size=1024):
        self.output_data = ([df[name] for name in df] for df in self._build_result(items, block_size))

    def output(self):
        return self.output_data

    def noops_fetch(self):
        for item in self.fetch_once():
            pass

    def fetch_once_as_dataframe(self):
        for items in self.fetch_once():
            yield pd.DataFrame(items)

    def fetch_once_as_rows(self):
        for df in self.fetch_once_as_dataframe():
            for index, row in df.iterrows():
                yield row

    def fetch_once_as_batch_rows(self):
        for df in self.fetch_once_as_dataframe():
            yield (row for index, row in df.iterrows())

    def fetch_once(self):
        if self.have_fetched:
            raise Exception("input data can only be fetched once")
        self.have_fetched = True
        for item in self.input_data:
            yield item.to_pydict()


class PythonProjectContext(object):
    def __init__(self):
        self.params_read = False
        self.conf = {}

    def read_params_once(self):
        self.params_read = True
        infile = sys.stdin.buffer
        for i in range(read_int(infile)):
            k = utf8_deserializer.loads(infile)
            v = utf8_deserializer.loads(infile)
            self.conf[k] = v

    def input_data_dir(self):
        return self.conf["tempDataLocalPath"]

    def output_model_dir(self):
        return self.conf["tempModelLocalPath"]


class RayContext(object):

    def __init__(self, python_context: PythonContext):
        self.python_context = python_context
        self.servers = []
        for item in self.python_context.fetch_once_as_rows():
            self.servers.append(DataServer(item["host"], int(item["port"]), item["timezone"]))

    def data_servers(self):
        return self.servers

    @staticmethod
    def fetch_data_from_single_data_server(data_server: DataServer):
        out_ser = ArrowStreamPandasSerializer(data_server.timezone, True, True)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((data_server.host, data_server.port))
            buffer_size = int(os.environ.get("BUFFER_SIZE", 65536))
            infile = os.fdopen(os.dup(sock.fileno()), "rb", buffer_size)
            result = out_ser.load_stream(infile)
            for items in result:
                yield items
