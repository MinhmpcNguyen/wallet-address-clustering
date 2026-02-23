import os.path
import time

# from multithread_processing.base_job import BaseJob
from constants.network_constants import get_chain_name
from constants.time_constants import TimeConstants
from databases.arangodb_klg import ArangoDB
from databases.mongodb import MongoDB
from utils.file_utils import (
    init_last_synced_file,
    read_last_synced_file,
    write_last_synced_file,
)
from utils.logger_utils import get_logger

REVERSECLAIMED = "REVERSECLAIMED"
RVERSENAMESET = "RVERSENAMESET"
SUFFIXES = {"0x38": "bnb", "0x1": "eth"}
SYNC_FILE_PATH = ".data/name_service_synced_file.txt"

logger = get_logger("Name Service Export Job (old)")


class MongoENSExportJob:
    """Multithread job to get export wallets that deposit into hot wallets during a time interval (usually 1 day)"""

    def __init__(
        self,
        chain_id,
        start_block: int = None,
        # end_block: int = None,
        batch_size: int = 10000,
        last_synced_file=SYNC_FILE_PATH,
        retry: bool = True,
        interval=TimeConstants.A_DAY,
    ):
        self.chain_id = chain_id
        self.mongo = MongoDB()
        self.arango = ArangoDB(prefix=get_chain_name(chain_id))

        self.start_block: int = 0
        self.last_synced_file: str = ""
        self.set_start_block_and_last_synced_file(start_block, last_synced_file)
        self.batch_size = batch_size

        self._retry = retry
        self._interval = interval

    def set_start_block_and_last_synced_file(self, start_block, last_synced_file):
        self.start_block = start_block
        self.last_synced_file = last_synced_file
        if (self.start_block is not None) or (
            not os.path.isfile(self.last_synced_file)
        ):
            _DEFAULT_START_BLOCK = self.mongo.get_min(
                col_name=f"name_events_{self.chain_id}", field_name="block_number"
            )
            init_last_synced_file(
                self.start_block or _DEFAULT_START_BLOCK, self.last_synced_file
            )
        self.start_block = read_last_synced_file(self.last_synced_file)

    def _export_names_data(self, from_block, to_block):
        names_w_address: dict[str, str] = dict()
        events = list(
            self.mongo.get_events_by_blocks_range(
                collection_name=f"name_events_{self.chain_id}",
                from_block=from_block,
                to_block=to_block,
                sort=1,
            )
        )
        for i, _event in enumerate(events):
            if _event["event_type"] == REVERSECLAIMED:
                for _event_2 in events[i + 1 :]:
                    if (
                        _event_2["node"] == _event["node"]
                        and _event_2["event_type"] == RVERSENAMESET
                    ):
                        names_w_address[_event_2["name"]] = _event["addr"]

        mongo_data = [
            {
                "chainId": self.chain_id,
                "name": _name,
                "address": _addr,
                "lastUpdatedAt": int(time.time()),
            }
            for _name, _addr in names_w_address.items()
        ]
        self.mongo.update_names(mongo_data)

        logger.info(
            f"Exported {len(names_w_address)} names from block {from_block} to {to_block} on {self.chain_id}"
        )

    def run(self):
        _latest_block = self.mongo.get_max(
            col_name=f"name_events_{self.chain_id}", field_name="block_number"
        )
        _from_block: int = self.start_block
        while True:
            _latest_block = self.mongo.get_max(
                col_name=f"name_events_{self.chain_id}", field_name="block_number"
            )
            _to_block = min(_from_block + self.batch_size, _latest_block)
            try:
                self._export_names_data(from_block=_from_block, to_block=_to_block)
                write_last_synced_file(self.last_synced_file, _to_block)
            except Exception as ex:
                self.handle_exception(ex)
                if self._retry:
                    self.retry()

            _from_block = _to_block

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
