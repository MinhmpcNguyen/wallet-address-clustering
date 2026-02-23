from clients.arango import AddrGraphClient
from clients.clickhouse import ClickHouseClient
from clients.mongo import MongoDBClient
from clients.mongo_entity import MongoDBEntityClient


class Clients:
    _mongo_client: MongoDBClient | None = None
    _arango_client: AddrGraphClient | None = None
    _clickhouse_client: ClickHouseClient | None = None
    _mongo_entity_client: MongoDBEntityClient | None = None

    @classmethod
    def get_clickhouse_client(cls) -> ClickHouseClient:
        if not cls._clickhouse_client:
            cls._clickhouse_client = ClickHouseClient()
        return cls._clickhouse_client

    @classmethod
    def get_arango_client(cls) -> AddrGraphClient:
        if not cls._arango_client:
            cls._arango_client = AddrGraphClient()
        return cls._arango_client

    @classmethod
    def get_mongo_client(cls) -> MongoDBClient:
        if not cls._mongo_client:
            cls._mongo_client = MongoDBClient()
        return cls._mongo_client

    @classmethod
    def get_mongo_entity_client(cls) -> MongoDBEntityClient:
        if not cls._mongo_entity_client:
            cls._mongo_entity_client = MongoDBEntityClient()
        return cls._mongo_entity_client

    @staticmethod
    def close_all() -> None:
        """
        Close all known services (called from lifespan shutdown).
        Safe to call multiple times.
        """
        try:
            MongoDBClient.close_mongodb_service()
        except Exception as e:
            print(f"[shutdown] MongoDB close failed: {e}")

        try:
            MongoDBEntityClient.close_mongodb_entity_service()
        except Exception as e:
            print(f"[shutdown] MongoDBEntity close failed: {e}")
