import sys

if sys.version >= '3':
    basestring = str
else:
    pass
from pyjava.serializers import read_int
from pyjava.utils import utf8_deserializer
import pandas as pd


class PythonContext(object):

    def __init__(self, iterator, conf):
        self.input_data = iterator
        self.output_data = [[]]
        self.conf = conf
        self.schema = ""
        self.have_fetched = False

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
