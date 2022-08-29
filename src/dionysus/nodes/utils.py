import itertools
from random import randint
from typing import Dict, Iterator, List


def return_gen_randint(start: int, end: int) -> Iterator[int]:
    for _ in itertools.count():
        yield randint(start, end)


def group_dict_keys_by_value(d: Dict[int, str]) -> Dict[str, List[int]]:
    """
    >>> a_dict = {0: "House of the Dragon", 1:
    "House of the Dragon", 2: "Ring of Power"}
    >>> group_dict_keys_by_value(d=a_dict)
    >>> {"House of the Dragon": [0, 1], "Ring of Power": [2]}
    """
    res: Dict[str, List[int]] = {}
    for i, v in d.items():
        res[v] = [i] if v not in res.keys() else res[v] + [i]

    return res
