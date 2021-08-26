import logging
import os
import socket
import time
import uuid

import pandas as pd
import sys

import pyjava.utils as utils
import requests
from pyjava.serializers import ArrowStreamSerializer
from pyjava.serializers import read_int
from pyjava.utils import utf8_deserializer
from pyjava.storage import streaming_tar

if sys.version >= '3':
    basestring = str
else:
    pass


class DataServer(object):
    def __init__(self, host, port, timezone):
        self.host = host
        self.port = port
        self.timezone = timezone


class LogClient(object):
    def __init__(self, conf):
        self.conf = conf
        if 'spark.mlsql.log.driver.url' in self.conf:
            self.url = self.conf['spark.mlsql.log.driver.url']
            self.log_user = self.conf['PY_EXECUTE_USER']
            self.log_token = self.conf['spark.mlsql.log.driver.token']
            self.log_group_id = self.conf['groupId']

    def log_to_driver(self, msg):
        if 'spark.mlsql.log.driver.url' not in self.conf:
            if self.conf['PY_EXECUTE_USER'] and self.conf['groupId']:
                logging.info("[owner] [{}] [groupId] [{}] __MMMMMM__ {}".format(self.conf['PY_EXECUTE_USER'],
                                                                                self.conf['groupId'], msg))
            else:
                logging.info(msg)
            return
        import json
        resp = json.dumps(
            {"sendLog": {
                "token": self.log_token,
                "logLine": "[owner] [{}] [groupId] [{}] __MMMMMM__ {}".format(self.log_user, self.log_group_id, msg)
            }}, ensure_ascii=False)
        requests.post(self.url, data=resp, headers={'content-type': 'application/x-www-form-urlencoded;charset=UTF-8'})

    def close(self):
        if hasattr(self, "conn"):
            self.conn.close()
            self.conn = None


class PythonContext(object):
    cache = {}

    def __init__(self, context_id, iterator, conf):
        self.context_id = context_id
        self.data_mmap_file_ref = {}
        self.input_data = iterator
        self.output_data = [[]]
        self.conf = conf
        self.schema = ""
        self.have_fetched = False
        self.log_client = LogClient(self.conf)
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
                df = pd.DataFrame(buffer, columns=buffer[0].keys())
                buffer.clear()
                yield df

        if len(buffer) > 0:
            df = pd.DataFrame(buffer, columns=buffer[0].keys())
            buffer.clear()
            yield df

    def build_result(self, items, block_size=1024):
        self.output_data = ([df[name] for name in df]
                            for df in PythonContext.build_chunk_result(items, block_size))

    def build_result_from_dir(self, target_dir, block_size=1024):
        items = streaming_tar.build_rows_from_file(target_dir)
        self.build_result(items, block_size)

    def output(self):
        return self.output_data

    def __del__(self):
        logging.info("==clean== context")
        if self.log_client is not None:
            try:
                self.log_client.close()
            except Exception as e:
                pass

        if 'data_mmap_file_ref' in self.data_mmap_file_ref:
            try:
                self.data_mmap_file_ref['data_mmap_file_ref'].close()
            except Exception as e:
                pass

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

    def fetch_as_dir(self, target_dir):
        if len(self.data_servers()) > 1:
            raise Exception("Please make sure you have only one partition on Java/Spark Side")
        items = self.fetch_once_as_rows()
        streaming_tar.save_rows_as_file(items, target_dir)


class PythonProjectContext(object):
    def __init__(self):
        self.params_read = False
        self.conf = {}
        self.read_params_once()
        self.log_client = LogClient(self.conf)

    def read_params_once(self):
        if not self.params_read:
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

    def __del__(self):
        self.log_client.close()


class RayContext(object):
    cache = {}
    conn_cache = {}

    def __init__(self, python_context):
        self.python_context = python_context
        self.servers = []
        self.server_ids_in_ray = []
        self.is_setup = False
        self.is_dev = utils.is_dev()
        self.is_in_mlsql = True
        self.mock_data = []
        if "directData" not in python_context.conf:
            for item in self.python_context.fetch_once_as_rows():
                self.server_ids_in_ray.append(str(uuid.uuid4()))
                self.servers.append(DataServer(
                    item["host"], int(item["port"]), item["timezone"]))

    def data_servers(self):
        return self.servers

    def conf(self):
        return self.python_context.conf

    def data_servers_in_ray(self):
        import ray
        from pyjava.rayfix import RayWrapper
        rayw = RayWrapper()
        for server_id in self.server_ids_in_ray:
            server = rayw.get_actor(server_id)
            yield ray.get(server.connect_info.remote())

    def build_servers_in_ray(self):
        from pyjava.rayfix import RayWrapper
        from pyjava.api.serve import RayDataServer
        import ray
        buffer = []
        rayw = RayWrapper()
        for (server_id, java_server) in zip(self.server_ids_in_ray, self.servers):
            # rds = RayDataServer.options(name=server_id, detached=True, max_concurrency=2).remote(server_id, java_server,
            #                                                                                0,
            #                                                                                java_server.timezone)
            rds = rayw.options(RayDataServer, name=server_id, detached=True, max_concurrency=2).remote(server_id,
                                                                                                       java_server,
                                                                                                       0,
                                                                                                       java_server.timezone)
            res = ray.get(rds.connect_info.remote())
            logging.debug("build ray data server server_id:{} java_server: {} servers:{}".format(server_id,
                                                                                                 str(vars(
                                                                                                     java_server)),
                                                                                                 str(vars(res))))
            buffer.append(res)
        return buffer

    @staticmethod
    def connect(_context, url, **kwargs):
        if isinstance(_context, PythonContext):
            context = _context
        elif isinstance(_context, dict):
            if 'context' in _context:
                context = _context['context']
            else:
                '''
                we are not in MLSQL
                '''
                context = PythonContext("", [], {"pythonMode": "ray"})
                context.rayContext.is_in_mlsql = False
        else:
            raise Exception("context is not set")

        if url is not None:
            from pyjava.rayfix import RayWrapper
            ray = RayWrapper()
            is_udf_client = context.conf.get("UDF_CLIENT")
            if is_udf_client is None:
                ray.shutdown()
                ray.init(url, **kwargs)
            if is_udf_client and url not in RayContext.conn_cache:
                ray.init(url, **kwargs)
                RayContext.conn_cache[url] = 1
        return context.rayContext

    def setup(self, func_for_row, func_for_rows=None):
        if self.is_setup:
            raise ValueError("setup can be only invoke once")
        self.is_setup = True
        import ray
        from pyjava.rayfix import RayWrapper
        rayw = RayWrapper()

        if not self.is_in_mlsql:
            if func_for_rows is not None:
                func = ray.remote(func_for_rows)
                return ray.get(func.remote(self.mock_data))
            else:
                func = ray.remote(func_for_row)

                def iter_all(rows):
                    return [ray.get(func.remote(row)) for row in rows]

                iter_all_func = ray.remote(iter_all)
                return ray.get(iter_all_func.remote(self.mock_data))

        buffer = []
        for server_info in self.build_servers_in_ray():
            server = rayw.get_actor(server_info.server_id)
            rci = ray.get(server.connect_info.remote())
            buffer.append(rci)
            server.serve.remote(func_for_row, func_for_rows)
        items = [vars(server) for server in buffer]
        self.python_context.build_result(items, 1024)
        return buffer

    def foreach(self, func_for_row):
        return self.setup(func_for_row)

    def map_iter(self, func_for_rows):
        return self.setup(None, func_for_rows)

    def collect(self):
        for shard in self.data_servers():
            for row in RayContext.fetch_once_as_rows(shard):
                yield row

    def fetch_as_dir(self, target_dir, servers=None):
        if not servers:
            servers = self.data_servers()
        if len(servers) > 1:
            raise Exception("Please make sure you have only one partition on Java/Spark Side")

        items = RayContext.collect_from(servers)
        streaming_tar.save_rows_as_file(items, target_dir)

    def build_result(self, items, block_size=1024):
        self.python_context.build_result(items, block_size)

    def build_result_from_dir(self, target_path):
        self.python_context.build_result_from_dir(target_path)

    @staticmethod
    def parse_servers(host_ports):
        hosts = host_ports.split(",")
        hosts = [item.split(":") for item in hosts]
        return [DataServer(item[0], int(item[1]), "") for item in hosts]

    @staticmethod
    def fetch_as_repeatable_file(context_id, data_servers, file_ref, batch_size):
        import pyarrow as pa

        def inner_fetch():
            for data_server in data_servers:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    out_ser = ArrowStreamSerializer()
                    sock.connect((data_server.host, data_server.port))
                    buffer_size = int(os.environ.get("BUFFER_SIZE", 65536))
                    infile = os.fdopen(os.dup(sock.fileno()), "rb", buffer_size)
                    result = out_ser.load_stream(infile)
                    for batch in result:
                        yield batch

        def gen_by_batch():
            import numpy as np
            import math
            if 'data_mmap_file_ref' not in file_ref:
                file_ref['data_mmap_file_ref'] = pa.memory_map(context_id + "/__input__.dat")
            reader = pa.ipc.open_file(file_ref['data_mmap_file_ref'])
            num_record_batches = reader.num_record_batches
            for i in range(num_record_batches):
                df = reader.get_batch(i).to_pandas()
                for small_batch in np.array_split(df, math.floor(df.shape[0] / batch_size)):
                    yield small_batch

        if 'data_mmap_file_ref' in file_ref:
            return gen_by_batch()
        else:
            writer = None
            for batch in inner_fetch():
                if writer is None:
                    writer = pa.RecordBatchFileWriter(context_id + "/__input__.dat", batch.schema)
                writer.write_batch(batch)
            writer.close()
            return gen_by_batch()

    def collect_as_file(self, batch_size):
        data_servers = self.data_servers()
        python_context = self.python_context
        return RayContext.fetch_as_repeatable_file(python_context.context_id, data_servers,
                                                   python_context.data_mmap_file_ref,
                                                   batch_size)

    @staticmethod
    def collect_from(servers):
        for shard in servers:
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
