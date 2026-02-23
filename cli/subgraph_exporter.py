# run.py

from config import MongoDBConfig
from constants.network_constants import Chains
from databases.arangodb_klg import AddressGraphClient
from databases.mongodb import MongoDB
from jobs.subgraph_exporter_job_v2 import SubgraphExporterJob
from utils.logger_utils import get_logger

logger = get_logger("Subgraphs Exporter")


def subgraph_exporter(
    chain: str = "ethereum",  # -c
    radius: int = 2,  # -r
    batch_size: int = 3600,  # -b
    max_workers: int = 4,  # -w
) -> None:
    """Export subgraphs from ArangoDB to MongoDB (no-CLI version)."""
    chain_l = str(chain).lower()
    if chain_l not in Chains.mapping:
        raise ValueError(f"Chain {chain} is not supported")

    chain_id = Chains.mapping[chain_l]

    arangodb = AddressGraphClient(prefix=chain_l)
    mongodb = MongoDB(MongoDBConfig.CONNECTION_URL)

    cursor = mongodb.get_user_wallet_from_deposit_wallets(
        _filter={"chainId": chain_id}, projection={"_id": 1, "userWallets": 1}
    )
    addresses: list[str] = []
    for doc in cursor:
        addresses.extend(doc.get("userWallets", []))
    unique_addresses: list[str] = list(set(addresses))

    logger.info(
        f"Preparing to export subgraphs | chain={chain_l} (id={chain_id}) | radius={radius} | batch_size={batch_size} | workers={max_workers} | unique_addresses={len(unique_addresses)}."
    )

    job = SubgraphExporterJob(
        importer=arangodb,
        exporter=mongodb,
        chain_id=chain_id,
        addresses=unique_addresses,
        radius=radius,
        batch_size=batch_size,
        max_workers=max_workers,
    )
    job.run()


if __name__ == "__main__":
    subgraph_exporter()
