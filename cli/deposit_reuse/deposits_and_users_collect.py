# run.py

import os
import time

from cli_scheduler.scheduler_job import (  # pyright: ignore [reportMissingTypeStubs]
    SchedulerJob,
)
from pymongo.synchronous.cursor import Cursor
from typing_extensions import override

from config import MongoDBConfig
from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from databases.clickhouse import ClickHouseCentic
from databases.mongodb import MongoDB
from jobs.deposti_reuse.deposits_and_users_job_old import DepositsAndUsersJob
from schemas.mongo_db_schema import DepositUserDoc
from utils.file_utils import (
    init_last_synced_file,
    read_last_synced_file,
    write_last_synced_file,
)
from utils.logger_utils import get_logger
from utils.time_utils import human_readable_time, round_timestamp
from utils.utils import get_burn_wallets, get_hot_wallets

logger = get_logger("Deposits and Users wallets collect")


def deposits_and_users_collect(
    # Defaults mirror the CLI you provided
    last_synced_file: str = ".data/last_synced_deposits_users.txt",
    start_time: int = 1755799200,  # -s
    end_time: int = 1755799200,  # -e
    batch_size: int = 1000,  # -b
    max_workers: int = 4,  # -w
    chain: str = "ethereum",  # -c
    interval: int = 3600,  # --interval
    delay: int = 0,  # --delay
    run_now: bool = True,  # --run-now
    source: list[str] | None = None,  # --source (multiple)
) -> None:
    """Collect deposit wallets and user wallets (no-CLI version)."""
    chain_l = str(chain).lower()
    if chain_l not in Chains.mapping:
        raise ValueError(f"Chain {chain} is not supported")
    chain_id = Chains.mapping[chain_l]
    mongodb: MongoDB = MongoDB(MongoDBConfig.CONNECTION_URL)
    cassandra: ClickHouseCentic = ClickHouseCentic()
    job = DepositsUsersScheduler(
        chain_id=chain_id,
        start_timestamp=start_time,
        end_timestamp=end_time,
        batch_size=batch_size,
        max_workers=max_workers,
        last_synced_file=last_synced_file,
        sources=list(source) if source else None,
        run_now=run_now,
        interval=interval,
        delay=delay,
        mongodb=mongodb,
        cassandra=cassandra,
    )
    job.run()  # pyright: ignore [reportUnknownMemberType]


class DepositsUsersScheduler(SchedulerJob):
    """Continual job wrapper for DepositsAndUsersJob."""

    def __init__(
        self,
        chain_id: str,
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int,
        max_workers: int,
        interval: int,
        delay: int,
        run_now: bool,
        last_synced_file: str,
        mongodb: MongoDB,
        cassandra: ClickHouseCentic,
        sources: list[str] | None = None,
    ):
        self.start_timestamp: int = start_timestamp
        scheduler = f"^{run_now}@{interval}/{delay}${end_timestamp}#true"
        super().__init__(scheduler)  # pyright: ignore [reportUnknownMemberType]

        self.batch_size: int = batch_size
        self.max_workers: int = max_workers
        self.chain_id: str = chain_id
        self.last_synced_file: str = last_synced_file
        self.sources: list[str] = sources or ["transactions", "token_transfers"]

        # Needed in _start()
        self.interval: int = interval
        self.delay: int = delay
        self.mongodb: MongoDB = mongodb
        self.cassandra: ClickHouseCentic = cassandra
        self.next_synced_timestamp: int

    @override
    def _pre_start(self):
        if not os.path.isfile(self.last_synced_file):
            _DEFAULT_START_TIME = int(time.time() - TimeConstants.DAYS_30)
            init_last_synced_file(
                self.start_timestamp or _DEFAULT_START_TIME, self.last_synced_file
            )
        self.start_timestamp = read_last_synced_file(self.last_synced_file)

    @override
    def _start(self):
        self.next_synced_timestamp = (
            round_timestamp(self.start_timestamp + self.interval, self.interval)
            + self.delay
        )
        logger.info(
            f"Start execute from {human_readable_time(self.start_timestamp)} to {human_readable_time(self.next_synced_timestamp)}"
        )

    @override
    def _execute(self):
        _hot_wallets = get_hot_wallets(self.chain_id)
        _burn_wallets = get_burn_wallets(self.chain_id)

        job = DepositsAndUsersJob(
            cassandra=self.cassandra,
            mongodb=self.mongodb,
            chain_id=self.chain_id,
            hot_wallets=set(_hot_wallets),
            burn_wallets=set(_burn_wallets),
            start_timestamp=self.start_timestamp,
            end_timestamp=self.next_synced_timestamp,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            sources=self.sources,
        )
        job.run()

    @override
    def _end(self):
        updating_cursor: Cursor[DepositUserDoc] = (
            self.mongodb.get_user_wallet_from_deposit_wallets(
                _filter={"chainId": "0x1", "updating": 1},
                projection={"_id": 1, "userWallets": 1},
            )
        )
        updated_docs: list[DepositUserDoc] = []
        for doc in updating_cursor:
            doc["updating"] = 0
            doc["numUsers"] = len(doc["userWallets"])
            updated_docs.append(doc)

        self.mongodb.update_docs(collection_name="depositUsers", data=updated_docs)

        logger.info(
            f"Finished execute from {human_readable_time(self.start_timestamp)} to {human_readable_time(self.next_synced_timestamp)}"
        )

        self.start_timestamp = self.next_synced_timestamp
        write_last_synced_file(self.last_synced_file, self.start_timestamp)
        time.sleep(3)


if __name__ == "__main__":
    deposits_and_users_collect()
