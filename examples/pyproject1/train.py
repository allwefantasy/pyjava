from pyjava.api.mlsql import PythonProjectContext

context = PythonProjectContext()
context.read_params_once()
print(context.conf)

import time
print('foo', flush=True)
print("yes")
# time.sleep(10)
print("10 yes")
