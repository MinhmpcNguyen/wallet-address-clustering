from config import MongoDBConfig
from databases.mongodb import MongoDB


class MongoDBClient:
    _mongodb_service: MongoDB | None = None

    @classmethod
    def get_mongodb_service(cls) -> MongoDB:
        """
        Get an instance of HttpService.
        """
        if not cls._mongodb_service:
            cls._mongodb_service = MongoDB(MongoDBConfig.CONNECTION_URL)
        return cls._mongodb_service

    @classmethod
    def close_mongodb_service(cls) -> None:
        """
        Close the MongoDBEntity connection if it exists.
        """
        if cls._mongodb_service:
            try:
                cls._mongodb_service.connection.close()
            except Exception as e:
                # Optional: log or handle error during close
                print(f"Error while closing MongoDBEntity connection: {e}")
            finally:
                cls._mongodb_service = None
