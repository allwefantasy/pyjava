from distutils.version import StrictVersion

import ray
import logging


def last(func):
    func.__module__ = "pyjava_auto_generate__exec__"
    return func


class RayWrapper:

    def __init__(self):
        self.ray_version = StrictVersion(ray.__version__)
        self.ray_instance = ray

    def __getattr__(self, attr):
        return getattr(self.ray_instance, attr)

    def get_address(self):
        if self.ray_version >= StrictVersion('1.0.0'):
            return ray.get_runtime_context().worker.node_ip_address
        else:
            return ray.services.get_node_ip_address()

    def init(self, address):
        if self.ray_version >= StrictVersion('1.0.0'):
            ray.init(address=address)
        else:
            ray.init(redis_address=address)

    def shutdown(self):
        if self.ray_version >= StrictVersion('1.0.0'):
            ray.shutdown(_exiting_interpreter=False)
        else:
            ray.shutdown(exiting_interpreter=False)

    def options(self, actor_class, **kwargs):
        if 'detached' in kwargs and self.ray_version >= StrictVersion('1.0.0'):
            del kwargs['detached']
            kwargs['lifetime'] = 'detached'
        logging.debug(f"options: {kwargs}")
        return actor_class.options(**kwargs)

    def get_actor(self, name):
        if self.ray_version >= StrictVersion('1.0.0'):
            return ray.get_actor(name)
        else:
            return ray.experimental.get_actor(name)

