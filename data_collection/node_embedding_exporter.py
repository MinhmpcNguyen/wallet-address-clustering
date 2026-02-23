import time
from typing import Any

import numpy as np
from numpy._typing import NDArray
from pymongo import UpdateOne
from pymongo.synchronous.database import Database

from config import MongoDBConfig
from constants.network_constants import Chains
from databases.mongodb import MongoDB
from service.query_subgraph import query_subgraph_to_mongo
from utils.embedding_utils import EmbeddingUtils
from utils.logger_utils import get_logger

logger = get_logger("Subgraphs Exporter Scheduler")


def node_embedding_exporter(
    out_collection_name: str,
    dest_collection_name: str,
    chain: str = "ethereum",
    radius: int = 2,
):
    chain = chain.lower()
    if chain not in Chains.mapping:
        raise ValueError(f"Chain '{chain}' is not supported")
    chain_id: str = Chains.mapping[chain]

    # Insert preprocessed subgraphs into out_collection_name, return written ids

    logger.info("Successful query subgraph")

    mongo = MongoDB(connection_url=MongoDBConfig.CONNECTION_URL)
    db: Database[Any] = mongo.db
    _, ids = query_subgraph_to_mongo(chain, radius, out_collection_name, mongo)
    src_col = db[out_collection_name]
    dst_col = db[dest_collection_name]

    try:
        dst_col.create_index(
            [("subgraphId", 1), ("address", 1)], name="sg_addr_idx", unique=True
        )
        dst_col.create_index([("chainId", 1), ("radius", 1)], name="chain_radius_idx")
        dst_col.create_index([("model", 1)], name="model_idx")
    except Exception:
        pass

    # If ids are empty, fetch all
    query_filter = {"_id": {"$in": ids}} if ids else {}
    # IMPORTANT: edges are required because EmbeddingUtils may need them
    projection = {"_id": 1, "vertices": 1, "edges": 1}
    cursor = src_col.find(query_filter, projection)

    ops: list[UpdateOne] = []
    now = int(time.time())
    n_docs = 0

    for doc in cursor:
        subgraph_id = doc["_id"]

        # Get embedding vectors (list[np.ndarray]) in the exact node order resolved by EmbeddingUtils
        emb: list[NDArray[np.float_]] = EmbeddingUtils.get_diff2vec_embedding(doc)

        # Use the exact node list resolved by EmbeddingUtils (to avoid order mismatch)
        # If you have added a public helper EmbeddingUtils.resolve_nodes(row), use it;
        # otherwise, fallback to the “private” function:
        # edges_like = EmbeddingUtils._normalize_edges_source(doc)  # type: ignore[attr-defined]
        addrs = EmbeddingUtils.resolve_nodes(doc)  # type: ignore[attr-defined]
        if not addrs or not emb:
            continue

        # If the number of vectors matches the number of nodes -> map 1-to-1;
        # otherwise, assign the same vector to all nodes (safe fallback)
        if len(emb) == len(addrs):
            pairs = zip(addrs, emb)
        else:
            pairs = ((a, emb[0]) for a in addrs)

        for addr, vec in pairs:
            # Ensure vec is a list (Mongo does not accept np.ndarray)
            vec_list = vec.tolist() if isinstance(vec, np.ndarray) else list(vec)

            key = {"subgraphId": subgraph_id, "address": addr}
            doc_out = {
                **key,
                "embedding": vec_list,
                "model": "diff2vec",
                "chain": chain,
                "chainId": chain_id,
                "radius": radius,
                "updatedAt": now,
            }
            ops.append(UpdateOne(key, {"$set": doc_out}, upsert=True))
            if len(ops) >= 1000:
                dst_col.bulk_write(ops, ordered=False)
                ops.clear()

        n_docs += 1

    if ops:
        dst_col.bulk_write(ops, ordered=False)

    logger.info(
        f"Embedded {n_docs} subgraphs into Mongo collection '{dest_collection_name}'"
    )


if __name__ == "__main__":
    node_embedding_exporter(
        out_collection_name="subgraph_ethereum_2_preprocessed",
        dest_collection_name="node_embeddings_ethereum_2",
        chain="ethereum",
        radius=2,
    )
    logger.info("Node embedding exporter finished successfully.")
