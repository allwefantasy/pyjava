import os

import pandas as pd
import ray

from pyjava.api.mlsql import DataServer
from pyjava.api.serve import RayDataServer

os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
ray.init(redis_address="192.168.205.34:65208")

ddata = pd.DataFrame(data=[[1, 2, 3, 4], [2, 3, 4, 5]])

server_id = "wow1"

java_server = DataServer("127.0.0.1", 11111, "Asia/Harbin")
rds = RayDataServer.options(name=server_id, detached=True, max_concurrency=2).remote(server_id, java_server, 0,
                                                                                     "Asia/Harbin")
print(ray.get(rds.connect_info.remote()))


def echo(row):
    return row


rds.serve.remote(echo)

ray.experimental
rds = ray.experimental.get_actor("wow1")
print(vars(ray.get(rds.connect_info.remote())))
