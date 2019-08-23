from pyjava.api.mlsql import PythonProjectContext

context = PythonProjectContext()
context.read_params_once()
print(context.conf)
print("yes")
