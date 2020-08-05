#-*-coding:utf-8-*-
from time import time


class Timer:
    def __init__(self):
        self.time = 0

    def __enter__(self):
        self.time = time()

    def __exit__(self, exc_t, exc_v, trace):
        print("Elapsed %s seconds" % (time() - self.time))
