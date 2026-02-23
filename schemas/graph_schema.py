from typing import NotRequired, TypedDict

from databases.clickhouse import ClickHouseCentic
from databases.mongodb_entity import MongoDBEntity


class GraphExporterJobKwargs(TypedDict):
    chain_id: str
    sources: list[str]
    mongo_klg: MongoDBEntity
    # arangodb: Arango
    cassandra: ClickHouseCentic
    batch_size: int
    max_workers: int
    hot_wallets: set[str]
    burn_wallets: set[str]


class TransferToGraphSchema(TypedDict, total=False):
    _key: str
    _from: str
    _to: str
    tokenTransferLogs: dict[str, dict[int, dict[str, float | None]]]
    oldestTransferAt: NotRequired[int]
