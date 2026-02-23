from logging import Logger
from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.synchronous.cursor import Cursor

from config import MongoDBEntityConfig
from constants.coingecko_constants import CoingeckoConstant
from schemas.mongo_entity_schema import ConfigDoc, MultichainWalletDoc, SmartContractDoc
from utils.logger_utils import get_logger

logger: Logger = get_logger("MongoDB Entity")


class MongoDBEntity:
    def __init__(self, connection_url: str) -> None:
        self.connection_url: str = connection_url.split("@")[-1]

        # Client and DB typed with a union to satisfy generic parameters
        self.connection: MongoClient[Any] = MongoClient(connection_url)
        self._db: Database[Any] = self.connection[MongoDBEntityConfig.DATABASE]

        # Collections with precise schemas
        self._config_col: Collection[ConfigDoc] = self._db["configs"]
        self._multichain_wallets_col: Collection[MultichainWalletDoc] = self._db[
            "multichain_wallets"
        ]
        self._smart_contracts_col: Collection[SmartContractDoc] = self._db[
            "smart_contracts"
        ]

    # Not used yet
    # def get_native_token_price_change_logs(self, chain_id) -> Dict:
    #     _filter = {'_id': f"{chain_id}_{NATIVE_TOKENS[chain_id]}"}
    #     _projection = ['priceChangeLogs']
    #     return self._smart_contracts_col.find_one(filter=_filter, projection=_projection)

    def get_price_change_logs(
        self, chain_id: str, token_addresses: list[str]
    ) -> Cursor[SmartContractDoc]:
        _token_ids = [f"{chain_id}_{address}" for address in token_addresses]
        _filter = {"_id": {"$in": _token_ids}, "priceChangeLogs": {"$exists": 1}}
        _projection = ["priceChangeLogs"]
        return self._smart_contracts_col.find(filter=_filter, projection=_projection)

    def get_top_token(self, chain_id: str) -> list[str]:
        """
        Return top 2000 tokens having the highest Market Cap from Coingecko
        """
        cursor: Cursor[SmartContractDoc] = self._smart_contracts_col.find(
            {"idCoingecko": {"$exists": True}, "chainId": chain_id}, {"address": 1}
        )

        addresses = [
            doc["address"]
            for doc in cursor.limit(CoingeckoConstant.TOP_MARKETCAP_COINGECKO)
        ]
        return addresses

    def get_coingecko_ids(self, chain_id: str) -> dict[str, str]:
        _filter: dict[str, str | dict[str, int]] = {
            "chainId": chain_id,
            "idCoingecko": {"$exists": 1},
        }
        _projection: list[str] = ["_id", "chainId", "address", "idCoingecko"]
        _cursor: Cursor[SmartContractDoc] = self._smart_contracts_col.find(
            filter=_filter, projection=_projection
        )
        return {doc["address"]: doc.get("idCoingecko", "") for doc in _cursor}
