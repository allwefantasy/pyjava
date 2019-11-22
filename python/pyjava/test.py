from pyjava.api.mlsql import PythonContext
from pyjava.serializers import \
    ArrowStreamPandasSerializer

out_ser = ArrowStreamPandasSerializer(None, True, True)

conf = {}
input_data = [dict([('id', [9, 10, 11, 12, 13, 14]),
                    ('content', ['1', '2', 'w', 'e', '5', '4']),
                    ('label', [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])])]
data_manager = PythonContext(input_data, conf)


def process():
    for item in data_manager.fetch_once_as_rows():
        item["label"] = item["label"] + 1
        yield item


items = process()

data_manager.build_result(items, 1024)
outfile = open("/tmp/test.txt", "wb")
out_ser.dump_stream(data_manager.output(), outfile)
outfile.close()

infile = open("/tmp/test.txt", "rb")
kk = out_ser.load_stream(infile)

for item in kk:
    print(item)
