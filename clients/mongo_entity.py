from config import MongoDBEntityConfig
from databases.mongodb_entity import MongoDBEntity


class MongoDBEntityClient:
    _mongodb_entity_service: MongoDBEntity | None = None

    @classmethod
    def get_mongodb_entity_service(cls) -> MongoDBEntity:
        """
        Get an instance of HttpService.
        """
        if not cls._mongodb_entity_service:
            cls._mongodb_entity_service = MongoDBEntity(
                MongoDBEntityConfig.CONNECTION_URL
            )
        return cls._mongodb_entity_service

    @classmethod
    def close_mongodb_entity_service(cls) -> None:
        """
        Close the MongoDBEntity connection if it exists.
        """
        if cls._mongodb_entity_service:
            try:
                cls._mongodb_entity_service.connection.close()
            except Exception as e:
                print(f"Error while closing MongoDBEntity connection: {e}")
            finally:
                cls._mongodb_entity_service = None
