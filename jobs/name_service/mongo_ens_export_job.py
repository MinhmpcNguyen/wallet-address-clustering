import os.path
import time

from constants.network_constants import Networks, get_chain_name
from constants.time_constants import TimeConstants
from databases.blockchain_etl import BlockchainETL
from databases.mongodb import MongoDB
from ens import ENS
from ens.exceptions import InvalidName
from utils.file_utils import (
    init_last_synced_file,
    read_last_synced_file,
    write_last_synced_file,
)
from utils.logger_utils import get_file_handler, get_logger, logging
from web3 import HTTPProvider, Web3
from web3.middleware import ExtraDataToPOAMiddleware

ADDRESSCHANGED = "ADDRESSCHANGED"
NAMEREGISTERED = "NAMEREGISTERED"

SUFFIXES = {"0x38": "bnb", "0x1": "eth"}
SYNC_FILE_PATH = ".data/mongo_ens_export.txt"

logger = get_logger("Mongo ENS Export Job")


class MongoENSExportJob:
    """Multithread job to get export wallets that deposit into hot wallets during a time interval (usually 1 day)"""

    def __init__(
        self,
        chain_id,
        start_block: int | None = None,
        # end_block: int = None,
        batch_size: int = 10000,
        last_synced_file: str = SYNC_FILE_PATH,
        log_error: bool = True,
        retry: bool = True,
        interval: int = TimeConstants.A_DAY,
    ):
        """
        Args:
            self: Refer to the current object
            chain_id: Determine the chain to sync
            start_block: int: Set the start block for the sync
            # end_block: int: Specify the end block of the sync
            batch_size: int: Determine how many blocks to sync at a time
            last_synced_file: str: Set the path of the file that stores last synced block number
            log_error: bool: Log errors to a file
            retry: bool: Determine whether to retry when an error occurs
            interval: Set the interval time of retry
            : Set the start block number
        """
        self.chain_id: str = chain_id
        _chain_name = get_chain_name(chain_id)
        self.name_suffix: str = SUFFIXES[chain_id]

        self.mongo: MongoDB = MongoDB()

        if not _chain_name:
            raise ValueError(f"Invalid chain_id: {chain_id}")
        self.web3: Web3 = Web3(HTTPProvider(Networks.providers[_chain_name]))
        self.web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        if chain_id == "0x38":
            self.blockchain_etl = BlockchainETL()
        else:
            self.blockchain_etl = BlockchainETL(db_prefix=_chain_name)

        self.start_block: int = 0
        self.last_synced_file: str = ""
        self.set_start_block_and_last_synced_file(start_block, last_synced_file)

        if log_error:
            _fh = get_file_handler()
            _fh.setLevel(logging.ERROR)
            logger.addHandler(_fh)

        self.batch_size: int = batch_size

        self._retry: bool = retry
        self._interval: int = interval

    def set_start_block_and_last_synced_file(
        self, start_block: int, last_synced_file: str
    ):
        self.start_block = start_block
        self.last_synced_file = last_synced_file
        if not os.path.isfile(self.last_synced_file):
            _DEFAULT_START_BLOCK = self.mongo.get_min(
                col_name=f"name_events_{self.chain_id}", field_name="block_number"
            )
            init_last_synced_file(
                self.start_block or _DEFAULT_START_BLOCK, self.last_synced_file
            )
        self.start_block = read_last_synced_file(self.last_synced_file)

    def _export_names_data(self, from_block, to_block):
        block_timestamps: dict[int, int] = (
            self.blockchain_etl.get_block_number_to_timestamp(
                start_block=from_block, end_block=to_block
            )
        )

        events: list[dict] = list(
            self.mongo.get_events_by_blocks_range(
                collection_name=f"name_events_{self.chain_id}",
                from_block=from_block,
                to_block=to_block,
                sort=1,
            )
        )
        # {namehash: event}
        events_nameregistered: dict[str, dict] = dict()
        events_addresschanged: dict[str, dict] = dict()
        for _event in events:
            if _event["block_number"] not in block_timestamps.keys():
                block_timestamps[_event["block_number"]] = self.web3.eth.get_block(
                    _event["block_number"]
                )["timestamp"]

            if _event["event_type"] == NAMEREGISTERED:
                try:
                    _name = f"{_event['name']}.{self.name_suffix}"
                    _namehash: str = ENS.namehash(_name).hex()
                    events_nameregistered.update(
                        {
                            _namehash: {
                                "chainId": self.chain_id,
                                # 'address': None,
                                "name": _name,
                                "lastUpdatedAt": block_timestamps[
                                    _event["block_number"]
                                ],
                            }
                        }
                    )
                except InvalidName:
                    logger.exception(
                        f"InvalidName at tx {_event['transaction_hash']}, log {_event['log_index']}"
                    )
                    continue
            elif _event["event_type"] == ADDRESSCHANGED:
                events_addresschanged.update(
                    {
                        _event[
                            "node"
                        ]: {  # node in AddressChanged is namehash in nameRegistered
                            "address": _event["newAddress"],
                            "lastUpdatedAt": block_timestamps[_event["block_number"]],
                        }
                    }
                )

        # merge AddressChanged and NameRegistered before updating to mongo
        for _namehash, _nameregistered_event in events_nameregistered.items():
            _addresschanged_event = events_addresschanged.pop(_namehash, None)
            if _addresschanged_event:
                _nameregistered_event.update(
                    {
                        "address": _addresschanged_event["address"],
                        "lastUpdatedAt": _addresschanged_event["lastUpdatedAt"],
                    }
                )

        mongo_names_registered: list[dict[str, str]] = [
            {"_id": f"{self.chain_id}_{_namehash}", **_event}
            for _namehash, _event in events_nameregistered.items()
        ]

        mongo_address_changed: list[dict] = [
            {"_id": f"{self.chain_id}_{_namehash}", **_event}
            for _namehash, _event in events_addresschanged.items()
        ]

        self.mongo.update_registered_names(mongo_names_registered)
        self.mongo.update_addresschanged_names(mongo_address_changed)

        logger.info(
            f"Exported {len(mongo_names_registered)} + {len(mongo_address_changed)} "
            f"names from block {from_block} to {to_block} on {self.chain_id}"
        )

    def run(self):
        _latest_block = self.mongo.get_max(
            col_name=f"name_events_{self.chain_id}", field_name="block_number"
        )
        _from_block: int = self.start_block
        while True:
            try:
                _latest_block = self.mongo.get_max(
                    col_name=f"name_events_{self.chain_id}", field_name="block_number"
                )
                _to_block = min(_from_block + self.batch_size, _latest_block)
                self._export_names_data(from_block=_from_block, to_block=_to_block)
                write_last_synced_file(self.last_synced_file, _to_block)
                _from_block = _to_block
            except Exception as ex:
                self.handle_exception(ex)
                if self._retry:
                    self.retry()

            if _from_block >= _latest_block:
                self.wait_to_next_synced()

    @staticmethod
    def handle_exception(ex):
        logger.exception(ex)
        logger.warning("Something went wrong")

    @staticmethod
    def retry():
        # Do before retry
        logger.warning(f"Try again after {TimeConstants.A_MINUTE} seconds ...")
        time.sleep(3)

    def wait_to_next_synced(self):
        # Sleep to next execute time
        # time_sleep = self.next_synced_timestamp - time.time()
        # if time_sleep > 0:
        #     self.logger.info(f'Waiting {round(time_sleep, 3)} seconds to the next execute [{human_readable_time(self.next_synced_timestamp)}]')
        #     time.sleep(time_sleep)
        logger.info(f"Waiting for next sync... Sleep for {TimeConstants.A_DAY}s")
        time.sleep(TimeConstants.A_DAY)


# if __name__ == "__main__":
#     job = MongoENSExportJob(chain_id="0x38", start_block=19710611)
#     job.run()
