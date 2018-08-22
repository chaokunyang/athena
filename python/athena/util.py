# coding=utf-8
from functools import wraps
import sys
import os
import importlib
import logging
import re


def parse_args(args: list):
    args_list = list(args)
    params = {}
    for i in range(0, len(args_list), 2):
        key = args_list[i][2:]
        params[key] = args_list[i + 1]
    return params


def coroutine(func):
    """
    Decorator: primes `func` by advancing to first `yield`.

    incompatible with `yield from`
    """

    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen

    return primer


def prepare_packages(packages: list):
    for p in packages:
        sys.path.insert(0, p)


def import_code(qualified_name: str):
    if qualified_name.endswith(".py"):
        dir_path = os.path.dirname(qualified_name)
        file_name = os.path.basename(qualified_name)
        if dir_path not in sys.path:
            sys.path.insert(0, dir_path)
        return importlib.import_module(file_name[0:-3])
    else:
        try:
            return importlib.import_module(qualified_name)
        except ModuleNotFoundError:
            parts = qualified_name.rsplit('.', 1)
            module = importlib.import_module(parts[0])
            task_cls = getattr(module, parts[1])  # get class
            return task_cls()


def get_logger(name: str, level=logging.INFO,
               fmt='[python] %(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)s]'):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter(fmt)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


async def read(reader, length):
    data = b''
    while True:
        data += await reader.read(length)
        if len(data) == length:
            break
    return data
