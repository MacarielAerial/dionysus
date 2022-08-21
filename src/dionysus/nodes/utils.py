from random import randint
from typing import Iterator


def return_gen_randint(start: int, end: int, n_iter: int) -> Iterator[int]:
    for _ in range(n_iter):
        yield randint(start, end)
