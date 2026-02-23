# run.py


from jobs.name_service.mongo_ens_export_job import MongoENSExportJob

from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from utils.logger_utils import get_logger

SYNC_FILE_PATH = ".data/mongo_ens_export.txt"
logger = get_logger("Name Service Exporter")


def mongo_ens_export(
    chain: str = "ethereum",  # -c
    start_block: int = 100000,  # -b (in your CLI this was the block)
    batch_size: int = TimeConstants.AN_HOUR,  # --batch-size (not overridden in CLI)
    last_synced_file: str = ".data/0x1_names.txt",  # --last-synced-file
    interval: int = TimeConstants.A_DAY,  # --interval
    retry: bool = True,  # --retry
) -> None:
    """Export ENS-like name service data from chain to Mongo (no-CLI version)."""
    chain_l = str(chain).lower()
    if chain_l not in Chains.mapping:
        raise ValueError(f"Chain {chain} is not supported")

    chain_id = Chains.mapping[chain_l]

    job = MongoENSExportJob(
        chain_id=chain_id,
        start_block=start_block,
        batch_size=batch_size,
        last_synced_file=last_synced_file,
        retry=retry,
        interval=interval,
    )
    job.run()


if __name__ == "__main__":
    mongo_ens_export()
