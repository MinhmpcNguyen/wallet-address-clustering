from datetime import date, datetime, timezone

from web3 import Web3

from service.blockchain.graph_operations import BlockTimestampGraph, GraphOperations
from utils.ethereum_utils import OutOfBoundsError


class EthService(object):
    def __init__(self, web3: Web3):
        graph = BlockTimestampGraph(web3)
        self._graph_operations: GraphOperations = GraphOperations(graph)

    def get_block_for_timestamp(self, timestamp: float):
        try:
            block_bounds = self._graph_operations.get_bounds_for_y_coordinate(timestamp)
        except OutOfBoundsError:
            return "latest"
        return block_bounds[0]

    def get_block_range_for_date(self, date: date):
        start_datetime = datetime.combine(
            date, datetime.min.time().replace(tzinfo=timezone.utc)
        )
        end_datetime = datetime.combine(
            date, datetime.max.time().replace(tzinfo=timezone.utc)
        )
        return self.get_block_range_for_timestamps(
            start_datetime.timestamp(), end_datetime.timestamp()
        )

    def get_block_range_for_timestamps(
        self, start_timestamp: float, end_timestamp: float
    ):
        start_timestamp = int(start_timestamp)
        end_timestamp = int(end_timestamp)
        if start_timestamp > end_timestamp:
            raise ValueError(
                "start_timestamp must be greater or equal to end_timestamp"
            )

        try:
            start_block_bounds = self._graph_operations.get_bounds_for_y_coordinate(
                start_timestamp
            )
        except OutOfBoundsError:
            start_block_bounds = (0, 0)

        try:
            end_block_bounds = self._graph_operations.get_bounds_for_y_coordinate(
                end_timestamp
            )
        except OutOfBoundsError as e:
            raise OutOfBoundsError(
                "The existing blocks do not completely cover the given time range"
            ) from e

        if (
            start_block_bounds == end_block_bounds
            and start_block_bounds[0] != start_block_bounds[1]
        ):
            raise ValueError("The given timestamp range does not cover any blocks")

        start_block = start_block_bounds[1]
        end_block = end_block_bounds[0]

        # The genesis block has timestamp 0 but we include it with the 1st block.
        if start_block == 1:
            start_block = 0

        return start_block, end_block
