import os

import pandas as pd

os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
from pyjava.api.serve import OnceServer

ddata = pd.DataFrame(data=[[1, 2, 3, 4], [2, 3, 4, 5]])

server = OnceServer("127.0.0.1", 11111, "Asia/Harbin")
server.bind()
server.serve([{'id': 9, 'label': 1}])
