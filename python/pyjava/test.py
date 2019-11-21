from pyjava.api.mlsql import PythonContext

from pyjava.serializers import \
    ArrowStreamPandasSerializer

out_ser = ArrowStreamPandasSerializer(None, True, True)

conf = {}
input_data = [dict([('id', [9, 10, 11, 12, 13, 14]),
                    ('content', ['1', '2', 'w', 'e', '5', '4']),
                    ('label', [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])])]
data_manager = PythonContext(input_data, conf)


def gen(_data_manager):
    for item in input_data:
        idCol = item["id"]
        contentCol = item["content"]
        labelCol = item["label"]
        yield [{"id": idCol[i] + 1, "content": contentCol[i], "label": labelCol[i]} for i in range(len(idCol))]


def outputdata(blocks):
    for block in blocks:
        import pandas as pd
        wow = pd.DataFrame(block)
        yield [wow['id'], wow['label']]


buffer = outputdata(gen(data_manager))
data_manager.have_fetched = True
data_manager.set_output(buffer)
outfile = open("/tmp/test.txt", "wb")
out_ser.dump_stream(data_manager.output(), outfile)
outfile.close()

infile = open("/tmp/test.txt", "rb")
kk = out_ser.load_stream(infile)

for item in kk:
    print(item)
