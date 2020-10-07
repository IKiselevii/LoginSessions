import time
import tracemalloc


def memory_logger(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        tracemalloc.start()
        res = func(*args, **kwargs)
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        print('Completed in:', round(time.time() - start_time, 1), f'seconds. Peak memory usage was: {peak / 10 ** 6}')
        return res
    return wrapper
