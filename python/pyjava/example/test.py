import ray

ray.init()


@ray.remote
def slow_function():
    return 1


print(ray.get(slow_function.remote()))
