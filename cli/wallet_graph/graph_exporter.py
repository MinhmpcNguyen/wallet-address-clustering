import os
import time
from typing import TypedDict

from cli_scheduler.scheduler_job import (  # pyright: ignore [reportMissingTypeStubs]
    SchedulerJob,
)
from config import MongoDBEntityConfig
from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from databases.clickhouse import ClickHouseCentic
from databases.mongodb_entity import MongoDBEntity
from jobs.wallet_graph.graph_exporter_job import GraphExporterJob
from schemas.graph_schema import GraphExporterJobKwargs
from typing_extensions import override
from utils.file_utils import (
    init_last_synced_file,
    read_last_synced_file,
    write_last_synced_file,
)
from utils.logger_utils import get_logger
from utils.time_utils import human_readable_time, round_timestamp
from utils.utils import get_burn_wallets, get_hot_wallets

HOT_WALLETS_PATH = "./artifacts/centralized_exchange_addresses.json"
BURN_WALLETS_PATH = "./artifacts/burn_wallets.json"
logger = get_logger("Graph Exporter Scheduler")


class TransferToGraphSchema(TypedDict):
    _key: str
    _from: str
    _to: str
    tokenTransferLogs: dict[str, dict[int, dict[str, float | None]]]


def graph_exporter(
    last_synced_file: str = ".data/last_synced_graph_exporter.txt",
    start_time: int = 1755972000,
    end_time: int = 1756144800,
    period: int = 900,  # 15 minutes = 900 seconds
    max_workers: int = 4,
    chain: str = "ethereum",
    interval: int = 86400,  # 3 days
    delay: int = 0,
    run_now: bool = True,
    source: list[str] | None = None,
):
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise ValueError(f"Chain {chain} is not supported")

    chain_id = Chains.mapping[chain]

    mongo_klg: MongoDBEntity = MongoDBEntity(MongoDBEntityConfig.CONNECTION_URL)
    cassandra: ClickHouseCentic = ClickHouseCentic()
    sources: list[str] = source if source else []
    # arangodb = _Arango(ArangoDBConfig.CONNECTION_URL, prefix=chain)
    graph_exporter_job_kwargs: GraphExporterJobKwargs = {
        "chain_id": chain_id,
        "sources": sources,
        "mongo_klg": mongo_klg,
        # "arangodb": arangodb,
        "cassandra": cassandra,
        "batch_size": period,
        "max_workers": max_workers,
        "hot_wallets": set(),
        "burn_wallets": set(),
    }

    job_graph_loader_scheduler = GraphLoaderSchedulerJob(
        chain_id=chain_id,
        interval=interval,
        delay=delay,
        run_now=run_now,
        last_synced_file=last_synced_file,
        start_timestamp=start_time,
        end_timestamp=end_time,
        graph_exporter_job_kwargs=graph_exporter_job_kwargs,
    )
    job_graph_loader_scheduler.run()  # pyright: ignore [reportUnknownMemberType]


class GraphLoaderSchedulerJob(SchedulerJob):
    def __init__(
        self,
        chain_id: str,
        interval: int,
        delay: int,
        run_now: bool,
        last_synced_file: str,
        graph_exporter_job_kwargs: GraphExporterJobKwargs,
        start_timestamp: int,
        end_timestamp: int | None = None,
    ):
        self.chain_id: str = chain_id
        self.start_timestamp: int = start_timestamp
        scheduler = f"^{run_now}@{interval}/{delay}${end_timestamp}#true"
        super().__init__(scheduler)  # pyright: ignore [reportUnknownMemberType]

        self.last_synced_file: str = last_synced_file
        self.interval: int = interval
        self.end_timestamp: int | None = end_timestamp
        self.graph_exporter_job_kwargs: GraphExporterJobKwargs = (
            graph_exporter_job_kwargs
        )
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
        self.next_synced_timestamp: int = (
            round_timestamp(self.start_timestamp + self.interval, self.interval)
            + self.delay
        )
        logger.info(
            f"Start exporting transfers to graph from {human_readable_time(self.start_timestamp)} to {human_readable_time(int(self.next_synced_timestamp))}"
        )

    @override
    def _execute(self):
        hot_wallets = set(get_hot_wallets(self.chain_id))
        burn_wallets = set(get_burn_wallets(self.chain_id))
        self.graph_exporter_job_kwargs.update(
            {"hot_wallets": hot_wallets, "burn_wallets": burn_wallets}
        )
        _graph_loader_job = GraphExporterJob(
            start_timestamp=self.start_timestamp,
            end_timestamp=self.next_synced_timestamp,
            **self.graph_exporter_job_kwargs,
        )
        _graph_loader_job.run()

    @override
    def _end(self):
        logger.info(
            "Finish exporting transfers to graph from "
            + human_readable_time(self.start_timestamp)
            + " to "
            + human_readable_time(self.next_synced_timestamp)
        )
        self.start_timestamp = self.next_synced_timestamp
        write_last_synced_file(self.last_synced_file, self.start_timestamp)
        time.sleep(3)


if __name__ == "__main__":
    graph_exporter()
