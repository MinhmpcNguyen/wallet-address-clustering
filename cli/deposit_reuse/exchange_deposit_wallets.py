# run.py

import json
import os
import time

from cli_scheduler.scheduler_job import (  # pyright: ignore [reportMissingTypeStubs]
    SchedulerJob,
)
from config import MongoDBConfig
from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from databases.clickhouse import ClickHouseCentic
from databases.mongodb import MongoDB
from jobs.deposti_reuse.exchange_deposit_wallets_job import ExchangeDepositWalletsJob
from typing_extensions import override
from utils.file_utils import (
    init_last_synced_file,
    read_last_synced_file,
    write_last_synced_file,
)
from utils.logger_utils import get_logger
from utils.time_utils import human_readable_time, round_timestamp

logger = get_logger("Exchange Deposit wallet")


def exchange_deposit_wallets(
    # Defaults mirror: -s 1750525200 -c ethereum --interval 86400 --run-now 1 -p 3600 -w 4
    last_synced_file: str = ".data/last_synced_deposit_wallets.txt",
    start_time: int = 1755972000,
    end_time: int = 1756231200,
    period: int = 3600,  # --period / -p
    max_workers: int = 4,  # --max-workers / -w
    chain: str = "ethereum",  # --chain / -c
    interval: int = 86400,  # --interval
    delay: int = 0,  # --delay
    run_now: bool = True,  # --run-now (1 -> True)
    source: list[str] | None = None,  # --source (multiple)
) -> None:
    """Get exchange trading information (no-CLI version)."""
    chain_l = str(chain).lower()
    if chain_l not in Chains.mapping:
        raise ValueError(f"Chain {chain} is not supported")
    chain_id = Chains.mapping[chain_l]

    cassandra = ClickHouseCentic()
    mongodb = MongoDB(MongoDBConfig.CONNECTION_URL)

    job = ExchangeWallets(
        cassandra=cassandra,
        mongodb=mongodb,
        chain_id=chain_id,
        start_timestamp=start_time,
        end_timestamp=end_time,
        period=period,
        interval=interval,
        delay=delay,
        run_now=run_now,
        max_workers=max_workers,
        last_synced_file=last_synced_file,
        sources=list(source) if source else None,
    )
    job.run()  # pyright: ignore [reportUnknownMemberType]


class ExchangeWallets(SchedulerJob):
    """Continual job wrapper for the ExchangeDepositWalletsJob."""

    def __init__(
        self,
        cassandra: ClickHouseCentic,
        mongodb: MongoDB,
        chain_id: str,
        start_timestamp: int,
        end_timestamp: int,
        period: int,
        interval: int,
        delay: int,  # Type annotation explicitly added
        run_now: bool,
        max_workers: int,
        last_synced_file: str,
        sources: list[str] | None,
    ):
        self.start_timestamp: int = start_timestamp
        scheduler = f"^{run_now}@{1}/{delay}${end_timestamp}#true"
        super().__init__(scheduler)  # pyright: ignore [reportUnknownMemberType]

        self.cassandra: ClickHouseCentic = cassandra
        self.period: int = period
        self.next_synced_timestamp: int  # Type annotation added
        self.interval: int = interval
        self.max_workers: int = max_workers

        self.chain_id: str = chain_id
        self.mongodb: MongoDB = mongodb
        self.last_synced_file: str = last_synced_file
        self.sources: list[str] = sources or ["transactions", "token_transfers"]
        self.delay: int = delay

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
            f"Start execute from {human_readable_time(self.start_timestamp)} to "
            + f"{human_readable_time(self.next_synced_timestamp)}"
        )

    @override
    def _execute(self):
        _exchange_wallets: dict[str, str] = self._get_exchange_wallets()
        _burn_wallet: list[str] = self._get_burn_wallets()
        _burn_wallets = set(_burn_wallet)
        job = ExchangeDepositWalletsJob(
            # databases & data
            cassandra=self.cassandra,
            exporter=self.mongodb,
            exchange_wallets=_exchange_wallets,
            burn_wallets=_burn_wallets,
            chain_id=self.chain_id,
            sources=self.sources,
            # time frame
            start_timestamp=self.start_timestamp,
            end_timestamp=self.next_synced_timestamp,
            # multi-workers
            period=self.period,
            batch_size=1,
            max_workers=self.max_workers,
        )
        job.run()

    @override
    def _end(self):
        self.start_timestamp = self.next_synced_timestamp
        write_last_synced_file(self.last_synced_file, self.start_timestamp)
        time.sleep(3)

    def _get_exchange_wallets(self):
        with open("artifacts/centralized_exchange_addresses.json") as f:
            centralized_exchanges = json.load(f)

        exchange_wallets: dict[str, str] = {}
        for exchange_id, info in centralized_exchanges.items():
            wallets = info.get("wallets", {})
            _ = exchange_wallets.update(
                {w.lower(): exchange_id for w in wallets.get(self.chain_id, [])}
            )
        return exchange_wallets

    def _get_burn_wallets(self) -> list[str]:
        with open("artifacts/burn_wallets.json") as f:
            burn_wallets_data = json.load(f)
        return [wallet.lower() for wallet in burn_wallets_data.get(self.chain_id, [])]


if __name__ == "__main__":
    exchange_deposit_wallets()
