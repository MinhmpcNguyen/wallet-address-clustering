import time
from typing import Any

from web3 import HTTPProvider, Web3
from web3.middleware import (
    ExtraDataToPOAMiddleware,  # pyright: ignore [reportUnknownVariableType]
)

from constants.network_constants import Chains, Networks
from constants.time_constants import TimeConstants
from service.blockchain.eth_services import EthService
from utils.logger_utils import get_logger

logger = get_logger("Blocks number")


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances: dict[Any, Any] = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class BlockNumber:
    def __init__(self, number: int, timestamp: int, ex: int = TimeConstants.A_DAY):
        self.number: int = number
        self.timestamp: int = timestamp
        self.expire: int = int(time.time()) + ex

    def is_expired(self) -> bool:
        return int(time.time()) > self.expire


class Blocks(metaclass=SingletonMeta):
    def __init__(self):
        self.eth_services: dict[Any, Any] = {}
        self.blocks: dict[str | int, dict[int, BlockNumber]] = {}
        for chain_id, chain_name in Chains.names.items():
            _w3 = Web3(HTTPProvider(Networks.providers[chain_name]))
            _w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)  # pyright: ignore [reportUnknownArgumentType]

            self.eth_services[chain_id] = EthService(_w3)
            self.blocks[chain_id] = {}

    # @sync_log_time_exe(tag=TimeExeTag.blockchain)
    def block_numbers(self, chain_id: str, timestamps: list[int]) -> dict[int, int]:
        if not timestamps:
            return {}

        # return_list = True

        if chain_id not in self.eth_services:
            raise ValueError(f"Chain {chain_id} not support")

        service = self.eth_services[chain_id]

        blocks: dict[int, int] = {}
        for timestamp in timestamps:
            if timestamp not in self.blocks:
                block_number = service.get_block_for_timestamp(timestamp)
                self.blocks[chain_id][timestamp] = BlockNumber(block_number, timestamp)

            block: BlockNumber = self.blocks[chain_id][timestamp]

            blocks[timestamp] = block.number

        self.clean(chain_id)
        # return blocks if return_list else blocks[timestamps[0]]
        return blocks

    def clean(self, chain_id: str):
        expired = []
        for timestamp, block in self.blocks[chain_id].items():
            if block.is_expired:
                expired.append(timestamp)

        for timestamp in expired:
            del self.blocks[chain_id][timestamp]
