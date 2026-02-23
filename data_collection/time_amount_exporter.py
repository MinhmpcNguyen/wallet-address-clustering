from config import MongoDBConfig, MongoDBEntityConfig
from constants.network_constants import Chains
from constants.time_constants import TimeConstants
from databases.arangodb_klg import AddressGraphClient
from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity
from service.query_subgraph import query_subgraph_to_mongo
from service.time_amount_exporter_service import TimeAmountExporterJob
from utils.logger_utils import get_logger

logger = get_logger("Time Amount Exporter Scheduler")


def time_amount_exporter(
    chain: str = "ethereum",
    radius: int = 2,
    batch_size: int = TimeConstants.AN_HOUR,
    max_workers: int = 8,
):
    chain = chain.lower()
    if chain not in Chains.mapping:
        raise ValueError(f"Chain '{chain}' is not supported")
    chain_id: str = Chains.mapping[chain]

    logger.info("Getting token list")
    token_list: list[str] = MongoDBEntity(
        connection_url=MongoDBEntityConfig.CONNECTION_URL
    ).get_top_token(chain_id=chain_id)

    logger.info("Preparing subgraph → Mongo (pure Python)")
    out_col = f"subgraph_{chain}_{radius}_preprocessed"

    count, ids = query_subgraph_to_mongo(
        chain=chain,
        radius=radius,
        out_collection_name=out_col,
        unique_key="_id",
        max_vertices=200,
        client=MongoDB(connection_url=MongoDBConfig.CONNECTION_URL),
    )
    logger.info(f"Prepared {count} subgraphs into '{out_col}'")

    arangodb: AddressGraphClient = AddressGraphClient(prefix=chain)

    job: TimeAmountExporterJob = TimeAmountExporterJob(
        chain=chain,
        chain_id=chain_id,
        list_index=None,
        token_list=token_list,
        transaction_database=arangodb.db,
        mongo_db=MongoDB(connection_url=MongoDBConfig.CONNECTION_URL),
        mongo_collection_prefix="time_amount_features",
        subgraph_collection_name=out_col,
        mongo_query_filter={"_id": {"$in": ids}},
        max_workers=max_workers,
        batch_size=batch_size,
    )
    job.run()


# if __name__ == "__main__":
#     # Example usage
#     time_amount_exporter(
#         saving_path="output",
#         chain="ethereum",
#         radius=2,
#         batch_size=100,
#         max_workers=4,
#     )
