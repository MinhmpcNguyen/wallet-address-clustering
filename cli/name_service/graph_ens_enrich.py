# run.py


from jobs.name_service.graph_ens_enrich_job import GraphENSEnrichJob

from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from utils.logger_utils import get_logger

SYNC_FILE_PATH = ".data/enrich_graph_name.txt"
logger = get_logger("Enrich Graph Names")


def graph_ens_enrich(
    chain: str = "ethereum",  # -c
    start_time: int | None = None,  # -s
    end_time: int | None = None,  # -e
    interval: int = TimeConstants.A_DAY,  # --interval
    run_now: bool = True,  # --run-now
    keep_old_names: bool = True,  # --keep-old-names 1
    last_synced_file: str = ".data/0x1_graph_ens.txt",  # --last-synced-file
) -> None:
    """Enrich graph with ENS-like names (no-CLI version)."""
    chain_l = str(chain).lower()
    if chain_l not in Chains.mapping:
        raise ValueError(f"Chain {chain} is not supported")

    chain_id = Chains.mapping[chain_l]

    job = GraphENSEnrichJob(
        chain_id=chain_id,
        start_timestamp=start_time,
        end_timestamp=end_time,
        interval=interval,
        run_now=run_now,
        keep_old_names=keep_old_names,
        last_synced_file=last_synced_file,
    )
    job.run()


if __name__ == "__main__":
    graph_ens_enrich()
