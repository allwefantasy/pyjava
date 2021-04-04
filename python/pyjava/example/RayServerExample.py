import os

import pandas as pd
import sys
sys.path.append("../../")

from pyjava.api.mlsql import DataServer
from pyjava.api.serve import RayDataServer
from pyjava.rayfix import RayWrapper

# import ray

os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
ray = RayWrapper()
ray.init(address='auto', _redis_password='5241590000000000')

ddata = pd.DataFrame(data=[[1, 2, 3, 4], [2, 3, 4, 5]])

server_id = "wow1"

java_server = DataServer("127.0.0.1", 11111, "Asia/Harbin")
rds = RayDataServer.options(name=server_id, max_concurrency=2).remote(server_id, java_server, 0,
                                                                      "Asia/Harbin")
print(ray.get(rds.connect_info.remote()))


def echo(row):
    return row


rds.serve.remote(echo)

rds = ray.get_actor(server_id)
print(vars(ray.get(rds.connect_info.remote())))
