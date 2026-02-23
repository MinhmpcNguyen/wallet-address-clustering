import os

# import sys
# sys.path.append(os.path.dirname(os.path.dirname(sys.path[0])))
import time

from cli_scheduler.scheduler_job import SchedulerJob

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
from utils.time_utils import human_readable_time, round_timestamp

logger = get_logger("Enrich Graph Name Job")


class GraphENSEnrichJob(SchedulerJob):
    def __init__(
        self,
        chain_id: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: int,
        last_synced_file: str,
        run_now: bool = True,
        keep_old_names: bool = False,
    ):
        """
        Get names data from Mongo, then push to Arango
        Args:
            chain_id: str: Specify the chain to run the job
            start_timestamp: int: Set the start time to get data from Mongo (using lastUpdatedAt field)
            end_timestamp: int: Set the end time to get data from Mongo (using lastUpdatedAt field)
            interval: int: Set the interval in seconds between each run of the job
            last_synced_file: str: Store the last synced timestamp
            run_now: bool: Determine if the sync should run immediately or wait for the interval to pass
            keep_old_names: bool: Remove old names from the Arango
            skip: int: number of name documents to skip on Mongo
        """
        self.start_timestamp = start_timestamp
        scheduler = f"^{run_now}@{interval}${end_timestamp}#true"
        super().__init__(scheduler)

        self.mongo = MongoDB()
        self.arango = ArangoDB(prefix=get_chain_name(chain_id))

        self.chain_id = chain_id

        self.last_synced_file = last_synced_file

        self.keep_old_names = keep_old_names

    def _pre_start(self):
        if (self.start_timestamp is not None) or (
            not os.path.isfile(self.last_synced_file)
        ):
            _DEFAULT_START_TIME = int(time.time() - TimeConstants.DAYS_30)
            init_last_synced_file(
                self.start_timestamp or _DEFAULT_START_TIME, self.last_synced_file
            )
        self.start_timestamp = read_last_synced_file(self.last_synced_file)

    def _start(self):
        self.next_synced_timestamp = (
            round_timestamp(self.start_timestamp + self.interval, self.interval)
            + self.delay
        )
        logger.info(
            f"Start execute from {human_readable_time(self.start_timestamp)} to "
            f"{human_readable_time(self.next_synced_timestamp)}"
        )

    def _execute(self, *args, **kwargs):
        _from_timestamp = self.start_timestamp
        _to_timestamp = self.next_synced_timestamp
        _count = 0
        names_data = self.mongo.get_names(
            timestamp_range=(_from_timestamp, _to_timestamp)
        )
        for _i, _datum in enumerate(names_data):
            if _datum.get("address") and self.arango.check_has_address(
                chain_id=self.chain_id, address=_datum["address"]
            ):
                _count += 1
                if not self.keep_old_names:
                    self.arango.delete_name(
                        chain_id=self.chain_id,
                        name=_datum["name"],
                        last_updated_at=_datum["lastUpdatedAt"],
                    )
                self.arango.update_name(
                    chain_id=self.chain_id,
                    address=_datum["address"],
                    name=_datum["name"],
                    last_updated_at=_datum["lastUpdatedAt"],
                )
        logger.info(f"Exported {_count} names to graph")

    def _end(self):
        self.start_timestamp = self.next_synced_timestamp
        write_last_synced_file(self.last_synced_file, self.start_timestamp)
        time.sleep(3)
