# run.py

from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from jobs.wallet_graph.graph_prune_job import GraphPruneJob
from utils.logger_utils import get_logger

logger = get_logger("Graph Prune")


def graph_prune(
    # Defaults mirror: -l .data/0x1_graph_prune.txt -B 100000 -b 100 -w 4 -c ethereum -t 120
    last_synced_file: str = ".data/0x1_graph_prune.txt",
    start_time: int | None = None,
    end_time: int | None = None,
    timespan: int = 120,
    batch_size_query: int = 100_000,
    max_workers: int = 4,
    batch_size_thread: int = 100,
    chain: str = "ethereum",
    interval: int = TimeConstants.A_DAY,
    delay: int = 0,
    run_now: bool = True,
) -> None:
    """Run graph prune job without CLI.

    Keeps only the most recent `timespan * interval` worth of data.
    """
    chain_l = str(chain).lower()
    if chain_l not in Chains.mapping:
        raise ValueError(f"Chain {chain} is not supported")

    chain_id = Chains.mapping[chain_l]

    job = GraphPruneJob(
        chain_id=chain_id,
        batch_size_query=batch_size_query,
        batch_size_thread=batch_size_thread,
        max_workers=max_workers,
        timespan=timespan,
        start_timestamp=start_time,
        end_timestamp=end_time,
        last_synced_file=last_synced_file,
        interval=interval,
        delay=delay,
        run_now=run_now,
    )
    job.run()  # pyright: ignore [reportUnknownMemberType]


if __name__ == "__main__":
    graph_prune()
