import os
import socket
import sys
import uuid

import pandas as pd

import pyjava.utils as utils
from pyjava.serializers import ArrowStreamSerializer
from pyjava.serializers import read_int
from pyjava.utils import utf8_deserializer

if sys.version >= '3':
    basestring = str
else:
    pass


class DataServer(object):
    def __init__(self, host, port, timezone):
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

    @staticmethod
    def build_chunk_result(items, block_size=1024):
        buffer = []
        for item in items:
            buffer.append(item)
            if len(buffer) == block_size:
                df = pd.DataFrame(buffer)
                buffer.clear()
                yield df

        if len(buffer) > 0:
            df = pd.DataFrame(buffer)
            buffer.clear()
            yield df

    def build_result(self, items, block_size=1024):
        self.output_data = ([df[name] for name in df] for df in PythonContext.build_chunk_result(items, block_size))

    def output(self):
        return self.output_data

    def noops_fetch(self):
        for item in self.fetch_once():
            pass

    def fetch_once_as_dataframe(self):
        for df in self.fetch_once():
            yield df

    def fetch_once_as_rows(self):
        for df in self.fetch_once_as_dataframe():
            for row in df.to_dict('records'):
                yield row

    def fetch_once_as_batch_rows(self):
        for df in self.fetch_once_as_dataframe():
            yield (row for row in df.to_dict('records'))

    def fetch_once(self):
        import pyarrow as pa
        if self.have_fetched:
            raise Exception("input data can only be fetched once")
        self.have_fetched = True
        for items in self.input_data:
            yield pa.Table.from_batches([items]).to_pandas()


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

    def __init__(self, python_context):
        self.python_context = python_context
        self.servers = []
        self.server_ids_in_ray = []
        self.is_setup = False
        self.rds_list = []
        self.is_dev = utils.is_dev()
        for item in self.python_context.fetch_once_as_rows():
            self.server_ids_in_ray.append(str(uuid.uuid4()))
            self.servers.append(DataServer(item["host"], int(item["port"]), item["timezone"]))

    def data_servers(self):
        return self.servers

    def data_servers_in_ray(self):
        import ray
        for server_id in self.server_ids_in_ray:
            server = ray.experimental.get_actor(server_id)
            yield ray.get(server.connect_info.remote())

    def build_servers_in_ray(self):
        import ray
        from pyjava.api.serve import RayDataServer
        buffer = []
        for (server_id, java_server) in zip(self.server_ids_in_ray, self.servers):

            rds = RayDataServer.options(name=server_id, detached=True, max_concurrency=2).remote(server_id, java_server,
                                                                                                 0,
                                                                                                 java_server.timezone)
            self.rds_list.append(rds)
            res = ray.get(rds.connect_info.remote())
            if self.is_dev:
                print("build RayDataServer server_id:{} java_server: {} servers:{}".format(server_id,
                                                                                           str(vars(java_server)),
                                                                                           str(vars(res))))
            buffer.append(res)
        return buffer

    @staticmethod
    def connect(context, url):
        import ray
        ray.shutdown(exiting_interpreter=False)
        ray.init(redis_address=url)
        return context.rayContext

    def setup(self, func_for_row, func_for_rows=None):
        if self.is_setup:
            raise ValueError("setup can be only invoke once")
        self.is_setup = True
        import ray
        buffer = []
        for server_info in self.build_servers_in_ray():
            server = ray.experimental.get_actor(server_info.server_id)
            buffer.append(ray.get(server.connect_info.remote()))
            server.serve.remote(func_for_row, func_for_rows)

        self.python_context.build_result([vars(server) for server in buffer], 1024)
        return buffer

    def foreach(self, func_for_row):
        return self.setup(func_for_row)

    def map_iter(self, func_for_rows):
        return self.setup(None, func_for_rows)

    def collect(self):
        for shard in self.data_servers():
            for row in RayContext.fetch_once_as_rows(shard):
                yield row

    def to_pandas(self):
        items = [row for row in self.collect()]
        return pd.DataFrame(data=items)

    @staticmethod
    def fetch_once_as_rows(data_server):
        for df in RayContext.fetch_data_from_single_data_server(data_server):
            for row in df.to_dict('records'):
                yield row

    @staticmethod
    def fetch_data_from_single_data_server(data_server):
        out_ser = ArrowStreamSerializer()
        import pyarrow as pa
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((data_server.host, data_server.port))
            buffer_size = int(os.environ.get("BUFFER_SIZE", 65536))
            infile = os.fdopen(os.dup(sock.fileno()), "rb", buffer_size)
            result = out_ser.load_stream(infile)
            for items in result:
                yield pa.Table.from_batches([items]).to_pandas()
