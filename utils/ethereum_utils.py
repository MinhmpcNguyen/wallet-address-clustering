import itertools
from collections.abc import Iterator

from typing_extensions import override


class Point(object):
    def __init__(self, x: int, y: float):
        self.x: int = x
        self.y: float = y

    @override
    def __str__(self):
        return "({},{})".format(self.x, self.y)

    @override
    def __repr__(self):
        return "Point({},{})".format(self.x, self.y)


def pairwise(iterable: list[Point]) -> Iterator[tuple[Point, Point]]:
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    _ = next(b, None)
    return zip(a, b)


class OutOfBoundsError(Exception):
    pass
