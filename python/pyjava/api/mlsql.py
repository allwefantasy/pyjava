class Data(object):

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
        have_fetched = True
        for item in self.input_data:
            yield item.to_pydict()
