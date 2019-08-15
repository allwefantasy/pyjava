class Data(object):

    def __init__(self, iterator):
        self.input_data = iterator
        self.output_data = None
        self.schema = ""

    def set_output(self, value, schema=""):
        self.output_data = value
        self.schema = schema

    def output(self):
        if self.output_data is None:
            raise Exception("You should call input_data.output(..) before finally return")
        return self.output_data

    def fetch_once(self):
        for item in self.input_data:
            yield item.to_pydict()
