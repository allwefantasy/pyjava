from __future__ import absolute_import

import ray
from pyjava.api.mlsql import PythonContext

ray.shutdown()

data = [{'id': 9, 'content': '1', 'label': 0.0}]
wow = PythonContext.build_chunk_result(data, 1024)
# items = ([df[name] for name in df] for df in wow)

for item in wow:
    print(item)
