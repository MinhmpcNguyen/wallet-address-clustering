# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from eth_typing import BlockIdentifier
from web3 import Web3
from web3.types import BlockData

from utils.ethereum_utils import OutOfBoundsError, Point, pairwise


class BlockTimestampGraph(object):
    def __init__(self, web3: Web3):
        self._web3: Web3 = web3

    def get_first_point(self):
        # Ignore the genesis block as its timestamp is 0
        return block_to_point(self._web3.eth.get_block(1))

    def get_last_point(self):
        return block_to_point(self._web3.eth.get_block("latest"))

    def get_point(self, x: BlockIdentifier):
        return block_to_point(self._web3.eth.get_block(x))


def block_to_point(block: BlockData) -> Point:
    return Point(int(block.get("number", 0)), int(block.get("timestamp", 0)))


class GraphOperations(object):
    def __init__(self, graph: BlockTimestampGraph):
        """x axis on the graph must be integers, y value must increase strictly monotonically with increase of x"""
        self._graph: BlockTimestampGraph = graph
        self._cached_points: list[Point] = []

    def get_bounds_for_y_coordinate(self, y: float):
        """given the y coordinate, outputs a pair of x coordinates for closest points that bound the y coordinate.
        Left and right bounds are equal in case given y is equal to one of the points y coordinate"""
        initial_bounds = find_best_bounds(y, self._cached_points)
        if initial_bounds is None:
            initial_bounds = self._get_first_point(), self._get_last_point()

        result = self._get_bounds_for_y_coordinate_recursive(y, *initial_bounds)
        return result

    def _get_bounds_for_y_coordinate_recursive(
        self, y: float, start: Point, end: Point
    ) -> tuple[int, int]:
        if y < start.y or y > end.y:
            raise OutOfBoundsError(
                "y coordinate {} is out of bounds for points {}-{}".format(
                    y, start, end
                )
            )

        if y == start.y:
            return start.x, start.x
        elif y == end.y:
            return end.x, end.x
        elif (end.x - start.x) <= 1:
            return start.x, end.x
        else:
            assert start.y < y < end.y
            if start.y >= end.y:
                raise ValueError("y must increase strictly monotonically")

            # Interpolation Search https://en.wikipedia.org/wiki/Interpolation_search, O(log(log(n)) average case.
            # Improvements for worst case:
            # Find the 1st estimation by linear interpolation from start and end points.
            # If the 1st estimation is below the needed y coordinate (graph is concave),
            # drop the next estimation by interpolating with the start and 1st estimation point
            # (likely will be above the needed y).
            # If 1st estimation is above the needed y coordinate (graph is convex),
            # drop the next estimation by interpolating with the 1st estimation and end point
            # (likely will be below the needed y).

            estimation1_x = interpolate(start, end, y)
            estimation1_x = bound(estimation1_x, (start.x, end.x))
            estimation1 = self._get_point(estimation1_x)

            if estimation1.y < y:
                points = (start, estimation1)
            else:
                points = (estimation1, end)

            estimation2_x = interpolate(*points, y)
            estimation2_x = bound(estimation2_x, (start.x, end.x))
            estimation2 = self._get_point(estimation2_x)

            all_points = [start, estimation1, estimation2, end]

            bounds = find_best_bounds(y, all_points)
            if bounds is None:
                raise ValueError(
                    "Unable to find bounds for points {} and y coordinate {}".format(
                        points, y
                    )
                )

            return self._get_bounds_for_y_coordinate_recursive(y, *bounds)

    def _get_point(self, x: int):
        point = self._graph.get_point(x)
        self._cached_points.append(point)
        return point

    def _get_first_point(self):
        point = self._graph.get_first_point()
        self._cached_points.append(point)
        return point

    def _get_last_point(self):
        point = self._graph.get_last_point()
        self._cached_points.append(point)
        return point


def find_best_bounds(y: float, points: list[Point]):
    sorted_points: list[Point] = sorted(points, key=lambda point: point.y)
    for point1, point2 in pairwise(sorted_points):
        if point1.y <= y <= point2.y:
            return point1, point2
    return None


def interpolate(point1: Point, point2: Point, y: float):
    x1, y1 = point1.x, point1.y
    x2, y2 = point2.x, point2.y
    if y1 == y2:
        # raise ValueError('The y coordinate for points is the same {}, {}'.format(point1, point2))
        return x1
    x = int((y - y1) * (x2 - x1) / (y2 - y1) + x1)
    return x


def bound(x: int, bounds: tuple[int, int]):
    x1, x2 = bounds
    if x1 > x2:
        x1, x2 = x2, x1
    if x <= x1:
        return x1 + 1
    elif x >= x2:
        return x2 - 1
    else:
        return x
