# data_collection/combine_features.py


from config import MongoDBConfig
from constants.network_constants import Chains
from databases.mongodb import MongoDB
from service.combine_features_service import ProcessTrainingDatasetMongo
from utils.logger_utils import get_logger


def combine_features_mongo(
    *,
    # INPUT collections
    from_col_name: str,  # e.g.: "time_amount_features_from"
    to_col_name: str,  # e.g.: "time_amount_features_to"
    embedding_col_name: str,  # e.g.: "node_embeddings_ethereum_2"
    pairs_col_name: str,  # e.g.: "deposit_reuse_pairs_ethereum"
    contracts_col_name: str
    | None = None,  # if filtering out contracts (has field IsContract)
    # OUTPUT collections
    out_train_col_name: str,  # e.g.: "train_data_ethereum_2"
    out_test_col_name: str,  # e.g.: "test_data_ethereum_2"
    mongo: MongoDB,
    # options
    chain: str = "ethereum",
    compute_embedding_similarity: bool = True,
    train_ratio: float = 0.9,
    balance_train_by_label: bool = True,
):
    """
    Combine features directly on Mongo, without using CSV/DataFrame.
    - Merge from/to -> Time[24]
    - Join embedding
    - Generate pair + Label from pairs collection
    - (optional) filter contracts
    - Compute cosine similarity embedding (optional)
    - Split train/test by X_address and write directly to 2 output collections
    """
    logger = get_logger("CombineFeatures(Mongo)")
    chain = chain.lower()
    chain_id = Chains.mapping.get(chain)
    if not chain_id:
        raise ValueError(f"Invalid chain '{chain}'")

    # Connect to Mongo

    db = mongo.db
    logger.info(
        f"DB: {db.name} | from={from_col_name}, to={to_col_name}, emb={embedding_col_name}, pairs={pairs_col_name}, out_train={out_train_col_name}, out_test={out_test_col_name}"
    )

    runner = ProcessTrainingDatasetMongo(
        db=db,
        from_col_name=from_col_name,
        to_col_name=to_col_name,
        embedding_col_name=embedding_col_name,
        pairs_col_name=pairs_col_name,
        contracts_col_name=contracts_col_name,
        out_train_col_name=out_train_col_name,
        out_test_col_name=out_test_col_name,
        compute_embedding_similarity=compute_embedding_similarity,
        train_ratio=train_ratio,
        chain_id=chain_id,
        balance_train_by_label=balance_train_by_label,
    )
    runner.run()
    logger.info("CombineFeatures(Mongo) finished.")


if __name__ == "__main__":
    combine_features_mongo(
        from_col_name="time_amount_features_from",
        to_col_name="time_amount_features_to",
        embedding_col_name="node_embeddings_ethereum_2",
        pairs_col_name="deposit_reuse_pairs_ethereum",
        contracts_col_name=None,  # filter smart_contract
        out_train_col_name="train_data_ethereum_2",
        out_test_col_name="test_data_ethereum_2",
        mongo=MongoDB(connection_url=MongoDBConfig.CONNECTION_URL),
        chain="ethereum",
        compute_embedding_similarity=True,
        train_ratio=0.9,
        balance_train_by_label=True,
    )
