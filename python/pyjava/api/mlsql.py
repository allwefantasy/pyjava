import sys

from pyjava.serializers import read_int
from pyjava.utils import utf8_deserializer


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

    def output(self):
        if not self.have_fetched:
            self.noops_fetch()
        return self.output_data

    def noops_fetch(self):
        for item in self.fetch_once():
            pass

    def fetch_once(self):
        if self.have_fetched:
            raise Exception("input data can only be fetched once")
        self.have_fetched = True
        for item in self.input_data:
            yield item.to_pydict()


class PythonProjectContext(object):
    def __init__(self):
        self.params_readed = False
        self.conf = {}

    def read_params_once(self):
        self.params_readed = True
        infile = sys.stdin
        for i in range(read_int(infile)):
            k = utf8_deserializer.loads(infile)
            v = utf8_deserializer.loads(infile)
            self.conf[k] = v

    def input_data_dir(self):
        return self.conf["tempDataLocalPath"]

    def output_model_dir(self):
        return self.conf["tempModelLocalPath"]
