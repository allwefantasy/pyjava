"""Owns an isolated real Ray cluster for the JVM Spark/Ray integration executable."""
import json
import os
from pathlib import Path
import sys
import tempfile
import shutil

import ray
from pyjava.api.mlsql import DataServer
from pyjava.api.serve import RayDataServer


def identity_batches(batches):
    yield from batches


def identity_rows(rows):
    yield from rows


def main():
    directory = Path(sys.argv[1]).resolve()
    actors = []
    runtime = tempfile.mkdtemp(prefix="pjr-", dir="/tmp")
    try:
        ray.init(address="local", num_cpus=2, include_dashboard=False,
                 object_store_memory=100 * 1024 * 1024,
                 _node_ip_address="127.0.0.1", _temp_dir=runtime, log_to_driver=False)
        mode = sys.argv[2]
        endpoints = []
        for i, line in enumerate((directory / "spark-sources.tsv").read_text().splitlines()):
            host, port = line.split("\t")
            actor = RayDataServer.options(num_cpus=1, max_concurrency=2).remote(
                "test-partition-%s" % i, DataServer(host, int(port), "UTC"))
            actors.append(actor)
            info = ray.get(actor.connect_info.remote(), timeout=60)
            if mode == "batches":
                actor.serve.remote(func_for_batches=identity_batches)
            else:
                actor.serve.remote(func_for_rows=identity_rows)
            endpoints.append("%s\t%s" % (info.host, info.port))
        temp = directory / "ray-endpoints.tmp"
        temp.write_text("\n".join(endpoints) + "\n")
        temp.replace(directory / "ray-endpoints.tsv")
        (directory / "ray-version.json").write_text(json.dumps({"ray": ray.__version__, "mode": mode}))
        sys.stdin.readline()  # The JVM owns this process and closes it in finally.
    finally:
        for actor in actors:
            ray.kill(actor, no_restart=True)
        ray.shutdown()
        shutil.rmtree(runtime, ignore_errors=True)


if __name__ == "__main__":
    main()
