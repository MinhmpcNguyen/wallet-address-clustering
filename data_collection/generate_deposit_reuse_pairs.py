from config import MongoDBConfig
from databases.mongodb import MongoDB
from service.deposit_reuse_pairs_job import DepositReusePairJob


def generate_deposit_reuse_pairs(
    chain_name: str,
    pairs_collection_name: str = "deposit_reuse_pairs_ethereum",
    max_workers: int = 2,
    batch_size: int = 1000,
):
    """Generate deposit reuse wallet pairs and save to file.

    Args:
        chain_name (str): Blockchain name (e.g., 'bsc', 'ethereum').
        file_path (str): Output CSV path to save pairs.
        max_workers (int): Number of parallel workers.
        batch_size (int): Batch size for processing.
    """

    job = DepositReusePairJob(
        chain=chain_name,
        mongo_db=MongoDB(connection_url=MongoDBConfig.CONNECTION_URL),
        refresh_number_sent_received=True,
        pairs_collection_name=pairs_collection_name,  # tùy chọn
        max_workers=max_workers,
        batch_size=batch_size,
    )
    job.run()


if __name__ == "__main__":
    generate_deposit_reuse_pairs(
        chain_name="ethereum",
        max_workers=4,
        batch_size=100,
    )
