from pymongo import MongoClient

from config import MongoDBSmartContractConfig
from utils.logger_utils import get_logger

logger = get_logger("MongoDB Entity")


# NO USE YET
class MongoDBSmartContract:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBSmartContractConfig.CONNECTION_URL

        self.connection_url = connection_url.split("@")[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBSmartContractConfig.DATABASE]
        self._projects_col = self._db["projects"]
        self._smart_contracts_col = self._db["smart_contracts"]
