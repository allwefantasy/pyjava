import logging
import os
import socket
from distutils.version import StrictVersion
import uuid

import pandas as pd
import sys
from typing import Dict

import pyjava.utils as utils
import requests
from pyjava.serializers import ArrowStreamSerializer
from pyjava.serializers import read_int
from pyjava.utils import utf8_deserializer
from pyjava.storage import streaming_tar

if sys.version >= '3':
    basestring = str
else:
    pass


def _actor_died(exc):
    name = type(exc).__name__
    return name in ("RayActorError", "ActorDiedError", "WorkerCrashedError",
                    "ActorUnavailableError")


def _generation_failure(exc, job, prepare_ms, deadline):
    """Keep a model exception. Add partition identity to a prepare timeout."""
    import time
    from pyjava.snapshot import SharedPrepareTimeout
    if isinstance(exc, SharedPrepareTimeout):
        return exc
    text = str(exc)
    timed_out = "python.socket.shared.prepare.timeout.ms" in text
    if timed_out or (_actor_died(exc) and time.monotonic() >= deadline):
        timeout = SharedPrepareTimeout(
            job["partition_id"], job["attempt_id"], prepare_ms)
        timeout.__cause__ = exc
        return timeout
    return exc


class DataServer(object):
    def __init__(self, host, port, timezone):
        self.host = host
        self.port = port
        self.timezone = timezone


class LogClient(object):
    def __init__(self, conf):
        self.conf = conf
        if 'spark.mlsql.log.driver.url' in self.conf:
            self.url = self.conf['spark.mlsql.log.driver.url']
            self.log_user = self.conf['PY_EXECUTE_USER']
            self.log_token = self.conf['spark.mlsql.log.driver.token']
            self.log_group_id = self.conf['groupId']

    def log_to_driver(self, msg):
        if 'spark.mlsql.log.driver.url' not in self.conf:
            if self.conf['PY_EXECUTE_USER'] and self.conf['groupId']:
                logging.info("[owner] [{}] [groupId] [{}] __MMMMMM__ {}".format(self.conf['PY_EXECUTE_USER'],
                                                                                self.conf['groupId'], msg))
            else:
                logging.info(msg)
            return
        import json
        resp = json.dumps(
            {"sendLog": {
                "token": self.log_token,
                "logLine": "[owner] [{}] [groupId] [{}] __MMMMMM__ {}".format(self.log_user, self.log_group_id, msg)
            }}, ensure_ascii=False)
        requests.post(self.url, data=resp, headers={'content-type': 'application/x-www-form-urlencoded;charset=UTF-8'})

    def close(self):
        if hasattr(self, "conn"):
            self.conn.close()
            self.conn = None


class PythonContext(object):
    cache = {}

    def __init__(self, context_id, iterator, conf):
        self.context_id = context_id
        self.data_mmap_file_ref = {}
        self.input_data = iterator
        self.output_data = [[]]
        self.conf = conf
        self.schema = ""
        self.have_fetched = False
        self.log_client = LogClient(self.conf)
        if "pythonMode" in conf and conf["pythonMode"] == "ray":
            self.rayContext = RayContext(self)

    def set_output(self, value, schema=""):
        self.output_data = value
        self.schema = schema

    @staticmethod
    def build_chunk_result(items, block_size=1024):
        buffer = []
        for item in items:
            buffer.append(item)
            if len(buffer) == block_size:
                df = pd.DataFrame(buffer, columns=buffer[0].keys())
                buffer.clear()
                yield df

        if len(buffer) > 0:
            df = pd.DataFrame(buffer, columns=buffer[0].keys())
            buffer.clear()
            yield df

    def build_result(self, items, block_size=1024):
        self.output_data = ([df[name] for name in df]
                            for df in PythonContext.build_chunk_result(items, block_size))

    def build_result_from_dir(self, target_dir, block_size=1024):
        items = streaming_tar.build_rows_from_file(target_dir)
        self.build_result(items, block_size)

    def output(self):
        return self.output_data

    def barrier(self):
        """Block until every task in this barrier stage reaches this call."""
        from pyjava.barrier import BarrierTaskContext
        BarrierTaskContext.get().barrier()

    def __del__(self):
        logging.info("==clean== context")
        if self.log_client is not None:
            try:
                self.log_client.close()
            except Exception as e:
                pass

        if 'data_mmap_file_ref' in self.data_mmap_file_ref:
            try:
                self.data_mmap_file_ref['data_mmap_file_ref'].close()
            except Exception as e:
                pass

    def noops_fetch(self):
        for item in self.fetch_once():
            pass

    def fetch_once_as_dataframe(self):
        for df in self.fetch_once():
            yield df

    def fetch_once_as_rows(self):
        for df in self.fetch_once_as_dataframe():
            for row in df.to_dict('records'):
                yield row

    def fetch_once_as_batch_rows(self):
        for df in self.fetch_once_as_dataframe():
            yield (row for row in df.to_dict('records'))

    def fetch_once(self):
        import pyarrow as pa
        if self.have_fetched:
            raise Exception("input data can only be fetched once")
        self.have_fetched = True
        for items in self.input_data:
            yield pa.Table.from_batches([items]).to_pandas()

    def fetch_as_dir(self, target_dir):
        if len(self.data_servers()) > 1:
            raise Exception("Please make sure you have only one partition on Java/Spark Side")
        items = self.fetch_once_as_rows()
        streaming_tar.save_rows_as_file(items, target_dir)


class PythonProjectContext(object):
    def __init__(self):
        self.params_read = False
        self.conf = {}
        self.read_params_once()
        self.log_client = LogClient(self.conf)

    def read_params_once(self):
        if not self.params_read:
            self.params_read = True
            infile = sys.stdin.buffer
            for i in range(read_int(infile)):
                k = utf8_deserializer.loads(infile)
                v = utf8_deserializer.loads(infile)
                self.conf[k] = v

    def input_data_dir(self):
        return self.conf["tempDataLocalPath"]

    def output_model_dir(self):
        return self.conf["tempModelLocalPath"]

    def __del__(self):
        self.log_client.close()


class RayContext(object):
    cache = {}
    conn_cache = {}

    def __init__(self, python_context):
        self.python_context = python_context
        self.servers = []
        self.server_ids_in_ray = []
        self.is_setup = False
        self.is_dev = utils.is_dev()
        self.is_in_mlsql = True
        self.mock_data = []
        if "directData" not in python_context.conf:
            for item in self.python_context.fetch_once_as_rows():
                self.server_ids_in_ray.append(str(uuid.uuid4()))
                self.servers.append(RayContext._to_data_server(item, self.conf()))

    @staticmethod
    def _to_data_server(item, conf=None):
        # Legacy rows are host/port/timezone only. A shared-transport job, or
        # any row that already carries a protocol or token, must be the
        # snapshot descriptor. Missing fields are an error, not a silent
        # fallback onto the one-shot Arrow socket.
        from pyjava.snapshot import PROTOCOL, SharedDataServer
        conf = conf or {}
        protocol = item.get("protocol")
        token = item.get("token")
        shared = conf.get("python.socket.transport") == "shared"
        if shared or protocol or token:
            if protocol != PROTOCOL or token is None or token == "":
                raise ValueError(
                    "shared snapshot input requires protocol %s and a token; "
                    "refusing to fall back to the legacy socket" % PROTOCOL)
            return SharedDataServer.from_row(item)
        return DataServer(item["host"], int(item["port"]), item["timezone"])

    def data_servers(self):
        return self.servers

    def conf(self):
        return self.python_context.conf

    def data_servers_in_ray(self):
        import ray
        from pyjava.rayfix import RayWrapper
        rayw = RayWrapper()
        for server_id in self.server_ids_in_ray:
            server = rayw.get_actor(server_id)
            yield ray.get(server.connect_info.remote())

    def build_servers_in_ray(self):
        from pyjava.rayfix import RayWrapper
        from pyjava.api.serve import RayDataServer
        import ray
        buffer = []
        rayw = RayWrapper()
        for (server_id, java_server) in zip(self.server_ids_in_ray, self.servers):
            # rds = RayDataServer.options(name=server_id, detached=True, max_concurrency=2).remote(server_id, java_server,
            #                                                                                0,
            #                                                                                java_server.timezone)
            rds = rayw.options(RayDataServer, name=server_id, detached=True, max_concurrency=2).remote(server_id,
                                                                                                       java_server,
                                                                                                       0,
                                                                                                       java_server.timezone)
            res = ray.get(rds.connect_info.remote())
            logging.debug("build ray data server server_id:{} java_server: {} servers:{}".format(server_id,
                                                                                                 str(vars(
                                                                                                     java_server)),
                                                                                                 str(vars(res))))
            buffer.append(res)
        return buffer

    @staticmethod
    def connect(_context, url, **kwargs):
        if isinstance(_context, PythonContext):
            context = _context
        elif isinstance(_context, dict):
            if 'context' in _context:
                context = _context['context']
            else:
                '''
                we are not in MLSQL
                '''
                context = PythonContext("", [], {"pythonMode": "ray"})
                context.rayContext.is_in_mlsql = False
        else:
            raise Exception("context is not detect. make sure it's in globals().")

        if url == "local":
            from pyjava.rayfix import RayWrapper
            ray = RayWrapper()
            if ray.ray_version < StrictVersion('1.6.0'):
                raise Exception("URL:local is only support in ray >= 1.6.0")
            # if not ray.ray_instance.is_initialized:
            ray.ray_instance.shutdown()
            ray.ray_instance.init(namespace="default")

        elif url is not None:
            from pyjava.rayfix import RayWrapper
            ray = RayWrapper()
            is_udf_client = context.conf.get("UDF_CLIENT")
            if is_udf_client is None:
                ray.shutdown()
                ray.init(url, **kwargs)
            if is_udf_client and url not in RayContext.conn_cache:
                ray.init(url, **kwargs)
                RayContext.conn_cache[url] = 1

        return context.rayContext

    def setup(self, func_for_row, func_for_rows=None, func_for_batches=None):
        if self.is_setup:
            raise ValueError("setup can be only invoke once")
        self.is_setup = True

        is_data_mode = "dataMode" in self.conf() and self.conf()["dataMode"] == "data"

        if not is_data_mode:
            raise Exception('''
Please setup dataMode as data instead of model. 
Try run: `!python conf "dataMode=data"` or 
add comment like: `#%dataMode=data` if you are in notebook.
            ''')

        import ray
        from pyjava.rayfix import RayWrapper
        rayw = RayWrapper()

        if not self.is_in_mlsql:
            if func_for_batches is not None:
                def apply_batches(items):
                    from pyjava.transfer import arrow_batches
                    return list(func_for_batches(arrow_batches(items)))
                return ray.get(ray.remote(apply_batches).remote(self.mock_data))
            if func_for_rows is not None:
                func = ray.remote(func_for_rows)
                return ray.get(func.remote(self.mock_data))
            else:
                func = ray.remote(func_for_row)

                def iter_all(rows):
                    return [ray.get(func.remote(row)) for row in rows]

                iter_all_func = ray.remote(iter_all)
                return ray.get(iter_all_func.remote(self.mock_data))

        if self.conf().get("python.socket.transport") == "shared":
            return self._setup_shared(rayw, func_for_row, func_for_rows, func_for_batches)

        buffer = []
        for server_info in self.build_servers_in_ray():
            server = rayw.get_actor(server_info.server_id)
            rci = ray.get(server.connect_info.remote())
            buffer.append(rci)
            server.serve.remote(func_for_row, func_for_rows, func_for_batches)
        items = [vars(server) for server in buffer]
        self.python_context.build_result(items, 1024)
        return buffer

    def _setup_shared(self, rayw, func_for_row, func_for_rows, func_for_batches):
        """Bounded resident snapshot actors. Does not call ray.shutdown.

        At most ``python.ray.inflight.generations`` (default 2) generations run
        at once. The permit is released when materialize returns, not when
        Spark later reads. Extra partitions are not queued onto the actors.
        Each submitted generation has its own prepare deadline, measured from
        ``generate.remote`` and covering scheduling, transform, and materialize.
        Leases start together after the last generation.
        """
        import time
        import ray
        from pyjava.api.serve import RaySnapshotWorker
        from pyjava.snapshot import (
            PROTOCOL, SharedDataServer, SharedPrepareTimeout, _conf_int,
            identity_value, snapshot_actor_workdir)

        inflight = _conf_int(self.conf(), "python.ray.inflight.generations",
                             "PYJAVA_INFLIGHT_GENERATIONS", 2)
        lease_ms = _conf_int(self.conf(), "python.socket.shared.lease.ms",
                             "PYJAVA_SNAPSHOT_LEASE_MS", 30 * 60 * 1000)
        prepare_ms = _conf_int(self.conf(), "python.socket.shared.prepare.timeout.ms",
                               "PYJAVA_SNAPSHOT_PREPARE_MS", lease_ms)
        raw_actors = self.conf().get("python.ray.snapshot.actors")
        count = len(self.servers)
        if count == 0:
            self.python_context.build_result([], 1024)
            return []
        if raw_actors is None:
            actor_count = min(count, inflight)
        else:
            requested = int(raw_actors)
            if requested <= 0:
                raise ValueError("python.ray.snapshot.actors must be positive")
            actor_count = min(count, requested)
        group = self.conf().get("python.ray.snapshot.group") or uuid.uuid4().hex
        actors = []
        names = []
        for index in range(actor_count):
            name = "pyjava-snapshot-%s-%d" % (group, index)
            names.append(name)
            # max_concurrency=2 lets abandon/shutdown run beside a stuck
            # generate. The second slot is not a second generation.
            actors.append(rayw.options(
                RaySnapshotWorker, name=name, detached=True, max_concurrency=2,
                num_cpus=0
            ).remote(name, dict(self.conf())))
        workdirs = [snapshot_actor_workdir(self.conf(), name) for name in names]
        try:
            ray.get([actor.endpoint.remote() for actor in actors], timeout=120)
        except BaseException:
            try:
                self._abort_snapshot_pool(actors, set(range(actor_count)), workdirs)
            except BaseException:
                logging.exception("failed to clean snapshot actors after startup error")
            raise

        jobs = []
        for index, java_server in enumerate(self.servers):
            jobs.append({
                "index": index,
                "server": java_server,
                "partition_id": identity_value(getattr(java_server, "partition_id", None), -1),
                "attempt_id": identity_value(getattr(java_server, "attempt_id", None), -1),
            })
        free = list(range(actor_count))
        pending = list(jobs)
        running = {}
        stuck = set()
        results = [None] * count
        succeeded = False
        last_keepalive = 0.0

        def submit():
            while pending and free and len(running) < inflight:
                slot = free.pop(0)
                job = pending.pop(0)
                ref = actors[slot].generate.remote(
                    job["server"], func_for_row, func_for_rows, func_for_batches,
                    job["partition_id"], job["attempt_id"])
                running[ref] = (slot, job, time.monotonic() + prepare_ms / 1000.0)

        def collect(done_refs):
            failure = None
            for ref in done_refs:
                slot, job, deadline = running.pop(ref)
                try:
                    results[job["index"]] = ray.get(ref)
                except Exception as exc:
                    if _actor_died(exc):
                        stuck.add(slot)
                    else:
                        free.append(slot)
                    if failure is None:
                        failure = _generation_failure(exc, job, prepare_ms, deadline)
                else:
                    free.append(slot)
            return failure

        try:
            submit()
            while running:
                remaining = min(item[2] - time.monotonic() for item in running.values())
                if remaining <= 0:
                    done, _pending_refs = ray.wait(
                        list(running.keys()), num_returns=1, timeout=0)
                else:
                    done, _pending_refs = ray.wait(
                        list(running.keys()), num_returns=1,
                        timeout=min(0.25, remaining))
                if done:
                    failure = collect(done)
                    if failure is not None:
                        raise failure
                    submit()
                    continue
                now = time.monotonic()
                overdue = [ref for ref, item in running.items() if now >= item[2]]
                if overdue:
                    failure = None
                    for ref in overdue:
                        slot, job, _deadline = running.pop(ref)
                        stuck.add(slot)
                        if failure is None:
                            failure = SharedPrepareTimeout(
                                job["partition_id"], job["attempt_id"], prepare_ms)
                    raise failure
                if free and now - last_keepalive >= 1.0 and remaining > 1.0:
                    last_keepalive = now
                    idle_actors = [actors[slot] for slot in list(free)]
                    try:
                        ray.wait(
                            [actor.keepalive.remote() for actor in idle_actors],
                            num_returns=len(idle_actors),
                            timeout=min(0.5, remaining))
                    except Exception:
                        logging.exception("snapshot keepalive failed")
            tokens = [row["token"] for row in results if row]
            deadlines = {}
            parts = ray.get(
                [actor.activate.remote(tokens) for actor in actors], timeout=10)
            for part in parts:
                deadlines.update(part)
            for row in results:
                if row.get("protocol") != PROTOCOL:
                    raise RuntimeError(
                        "shared snapshot descriptor protocol is %s, expected %s" %
                        (row.get("protocol"), PROTOCOL))
                deadline = deadlines.get(row["token"])
                if not deadline or int(deadline) <= 0:
                    raise RuntimeError("shared snapshot lease was not activated")
                row["lease_deadline_ms"] = int(deadline)
                if int(row["partition_id"]) < -1 or int(row["attempt_id"]) < -1:
                    raise RuntimeError("shared snapshot identity is invalid")
            self.python_context.build_result(results, 1024)
            succeeded = True
            return [SharedDataServer.from_row(row) for row in results]
        except BaseException:
            for item in running.values():
                stuck.add(item[0])
            try:
                self._abort_snapshot_pool(actors, stuck, workdirs)
            except BaseException:
                logging.exception("snapshot pool cleanup failed")
            raise
        finally:
            if succeeded:
                self._arm_snapshot_idle(actors)

    def _abort_snapshot_pool(self, actors, stuck, workdirs):
        """Drop this setup's actors and their snapshot directories.

        Does not call ``ray.shutdown`` and does not touch any other actor.
        A stuck ``generate`` is not asked to run ``shutdown`` on the same
        concurrency slot; ``abandon`` runs beside it, then ``ray.kill``.
        """
        import shutil
        import ray
        stuck = {slot for slot in stuck if isinstance(slot, int) and 0 <= slot < len(actors)}
        try:
            refs = []
            for slot in stuck:
                try:
                    refs.append(actors[slot].abandon.remote())
                except Exception:
                    logging.exception("failed to schedule snapshot abandon")
            if refs:
                try:
                    ray.wait(refs, num_returns=len(refs), timeout=2)
                except BaseException:
                    logging.exception("snapshot abandon wait failed")
        finally:
            for path in workdirs or []:
                shutil.rmtree(path, ignore_errors=True)
            for slot in stuck:
                self._kill_snapshot_actor(actors[slot])
            idle = [actor for index, actor in enumerate(actors) if index not in stuck]
            self._shutdown_or_kill(idle)
            for path in workdirs or []:
                shutil.rmtree(path, ignore_errors=True)

    def _shutdown_or_kill(self, actors):
        import ray
        if not actors:
            return
        try:
            ray.get([actor.shutdown.remote() for actor in actors], timeout=5)
        except BaseException as exc:
            # exit_actor() makes the shutdown RPC look like the worker died.
            if _actor_died(exc):
                return
            logging.exception("snapshot shutdown failed; killing these actors")
            for actor in actors:
                self._kill_snapshot_actor(actor)

    def _kill_snapshot_actor(self, actor):
        import ray
        try:
            ray.kill(actor, no_restart=True)
        except Exception:
            logging.exception("ray.kill snapshot actor failed")

    def _arm_snapshot_idle(self, actors):
        import ray
        try:
            ray.get([actor.enable_idle.remote() for actor in actors], timeout=5)
        except Exception:
            logging.exception("failed to arm snapshot actor idle TTL")

    def foreach(self, func_for_row):
        return self.setup(func_for_row)

    def map_iter(self, func_for_rows):
        return self.setup(None, func_for_rows)

    def map_batches(self, func_for_batches):
        """Transform an iterator of Arrow RecordBatches without pandas/row conversion."""
        return self.setup(None, None, func_for_batches)

    def collect(self):
        for shard in self.data_servers():
            for row in RayContext.fetch_once_as_rows(shard, self.conf()):
                yield row

    def fetch_as_dir(self, target_dir, servers=None):
        if not servers:
            servers = self.data_servers()
        if len(servers) > 1:
            raise Exception("Please make sure you have only one partition on Java/Spark Side")

        items = RayContext.collect_from(servers)
        streaming_tar.save_rows_as_file(items, target_dir)

    def build_result(self, items, block_size=1024):
        self.python_context.build_result(items, block_size)

    def build_result_from_dir(self, target_path):
        self.python_context.build_result_from_dir(target_path)

    @staticmethod
    def parse_servers(host_ports):
        hosts = host_ports.split(",")
        hosts = [item.split(":") for item in hosts]
        return [DataServer(item[0], int(item[1]), "") for item in hosts]

    @staticmethod
    def fetch_as_repeatable_file(context_id, data_servers, file_ref, batch_size):
        import pyarrow as pa

        def inner_fetch():
            for data_server in data_servers:
                for batch in RayContext.fetch_arrow_batches(data_server):
                    yield batch

        def gen_by_batch():
            import numpy as np
            import math
            if 'data_mmap_file_ref' not in file_ref:
                file_ref['data_mmap_file_ref'] = pa.memory_map(context_id + "/__input__.dat")
            reader = pa.ipc.open_file(file_ref['data_mmap_file_ref'])
            num_record_batches = reader.num_record_batches
            for i in range(num_record_batches):
                df = reader.get_batch(i).to_pandas()
                for small_batch in np.array_split(df, math.floor(df.shape[0] / batch_size)):
                    yield small_batch

        if 'data_mmap_file_ref' in file_ref:
            return gen_by_batch()
        else:
            writer = None
            for batch in inner_fetch():
                if writer is None:
                    writer = pa.RecordBatchFileWriter(context_id + "/__input__.dat", batch.schema)
                writer.write_batch(batch)
            writer.close()
            return gen_by_batch()

    def collect_as_file(self, batch_size):
        data_servers = self.data_servers()
        python_context = self.python_context
        return RayContext.fetch_as_repeatable_file(python_context.context_id, data_servers,
                                                   python_context.data_mmap_file_ref,
                                                   batch_size)

    @staticmethod
    def collect_from(servers, conf=None):
        for shard in servers:
            for row in RayContext.fetch_once_as_rows(shard, conf):
                yield row

    def to_pandas(self):
        items = [row for row in self.collect()]
        return pd.DataFrame(data=items)

    @staticmethod
    def fetch_once_as_rows(data_server, conf=None):
        # Converting nullable int64 through pandas would round values above 2**53.
        from pyjava.transfer import arrow_rows
        for batch in RayContext.fetch_arrow_batches(data_server, conf):
            yield from arrow_rows(batch)

    @staticmethod
    def fetch_arrow_batches(data_server, conf=None):
        """Stream typed Arrow batches; the caller must close an abandoned generator.

        SharedDataServer descriptors use the shared snapshot protocol; legacy
        DataServer keeps the raw Arrow stream behaviour.
        """
        import pyarrow as pa
        from pyjava.snapshot import PROTOCOL, fetch_shared_arrow_batches
        from pyjava.transfer import StrictArrowInput, positive_env
        if getattr(data_server, "protocol", "") == PROTOCOL and \
                getattr(data_server, "token", None):
            for batch in fetch_shared_arrow_batches(data_server, conf):
                yield batch
            return
        timeout = positive_env("PYJAVA_SOCKET_TIMEOUT_SECONDS", 300)
        with socket.create_connection((data_server.host, data_server.port), timeout=10) as sock:
            buffer_size = utils.configure_transfer_socket(sock)
            sock.settimeout(timeout)
            with sock.makefile("rb", buffer_size) as infile:
                with pa.ipc.open_stream(StrictArrowInput(infile)) as reader:
                    yield from reader

    @staticmethod
    def fetch_data_from_single_data_server(data_server, conf=None):
        for batch in RayContext.fetch_arrow_batches(data_server, conf):
            yield batch.to_pandas()
