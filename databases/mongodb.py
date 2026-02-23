import time
from typing import Any

from bson.objectid import ObjectId
from config import MongoDBConfig
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import InvalidOperation
from pymongo.synchronous.cursor import Cursor
from schemas.mongo_db_schema import (
    DepositUserDoc,
    DepositWalletDoc,
    GroupDoc,
    LPTokenDoc,
    TokenTransferDoc,
    TransactionDoc,
    UserDepositDoc,
)
from schemas.subgraph_schema import SubgraphDoc
from utils.file_utils import write_to_file
from utils.list_dict_utils import (
    delete_none,  # pyright: ignore [reportUnknownVariableType]
    flatten_dict,  # pyright: ignore [reportUnknownVariableType]
)
from utils.logger_utils import get_logger
from utils.time_utils import human_readable_time

ERROR_LOG_FILE = ".data/mongodb_errors.txt"
NO_OPERATION_TO_EXECUTE = "No operations to execute"
WALLETS_COL: str = "depositWallets"
logger = get_logger("MongoDB")


class MongoDB:
    def __init__(self, connection_url: str, chain_id: str = "") -> None:
        self.connection_url: str = connection_url.split("@")[-1]

        # MongoDB client
        self.connection: MongoClient[Any] = MongoClient(
            host=connection_url,
            timeoutMS=None,  # Disable CSOT (optional)
            socketTimeoutMS=0,  # Infinite socket timeout
            connectTimeoutMS=60000,  # 60s to connect
            serverSelectionTimeoutMS=120000,  # 120s to find a primary
            maxPoolSize=64,  # Increase connection pool size
        )

        # Database object
        self.db: Database[Any] = self.connection[MongoDBConfig.DATABASE]

        # Collections
        self.lp_tokens_col: Collection[LPTokenDoc] = self.db["lpTokens"]
        self._deposit_wallets_col: Collection[DepositWalletDoc] = self.db[
            "depositWallets"
        ]
        self._groups_col: Collection[GroupDoc] = self.db["groups"]
        self._deposit_users_col: Collection[DepositUserDoc] = self.db["depositUsers"]
        self._user_deposits_col: Collection[UserDepositDoc] = self.db["userDeposits"]
        self._transactions_col: Collection[TransactionDoc] = self.db[
            f"{chain_id}_transactions"
        ]
        self._token_transfers_col: Collection[TokenTransferDoc] = self.db[
            "token_transfers"
        ]
        # self._subgraphs_col = self.db["subgraphs"]
        # self._names_col = self.db["names"]

    #######################
    #      NO USE YET   #
    #######################
    #######################
    #       Generals      #
    #######################

    # def count_documents(self, col_name: str) -> int:
    #     return self._db[col_name].estimated_document_count()

    # def get_documents(
    #     self, col_name: str, skip: int, limit: int
    # ) -> Iterator[dict[str, Any]]:
    #     return self._db[col_name].find({}).skip(skip).limit(limit)

    # def get_documents_by_ids(
    #     self, col_name: str, ids: list[str]
    # ) -> Iterator[dict[str, str]]:
    #     return self._db[col_name].find({"_id": {"$in": ids}})

    # #######################
    # #   Deposit - Users   #
    # #######################

    # def get_deposit_number_of_users(
    #     self, skip: int = 0, limit: int = 1
    # ) -> Generator[dict[str, Any], None, None]:
    #     pipeline: list[dict[str, int | dict[str, dict[str, str]]]] = [
    #         {"$project": {"numberOfUsers": {"$size": "$userWallets"}}}
    #     ]
    #     if skip:
    #         pipeline.append({"$skip": skip})
    #     if limit:
    #         pipeline.append({"$limit": limit})
    #     return self._deposit_users_col.aggregate(pipeline=pipeline)

    # def get_deposit_wallet_by_users(
    #     self, chain_id: str, addresses: list[str]
    # ) -> Iterator[dict[str, Any]]:
    #     _ids: list[str] = [f"{chain_id}_{address}" for address in addresses]
    #     _filter: dict[str, Any] = {"_id": {"$in": _ids}}
    #     _projection: dict[str, int] = {"_id": 1, "depositWallets": 1, "address": 1}
    #     return self._user_deposits_col.find(_filter, _projection)

    # #######################
    # #  Project deployers  #
    # #######################

    # def update_project_deployers(
    #     self, chain_id: str, project_deployers: dict[str, list[str]]
    # ) -> None:
    #     bulk_updates: list[UpdateOne] = []
    #     for project, deployers in project_deployers.items():
    #         _id: str = f"{chain_id}_{project}"
    #         bulk_updates.append(
    #             UpdateOne(
    #                 filter={"_id": _id},
    #                 update={
    #                     "$set": {"project": project, "chainId": chain_id},
    #                     "$addToSet": {"deployers": {"$each": deployers}},
    #                 },
    #                 upsert=True,
    #             )
    #         )
    #     try:
    #         _ = self._db["projectDeployers"].bulk_write(bulk_updates)
    #     except InvalidOperation as ex:
    #         _message: str = ex.args[0]
    #         if _message != "No operations to execute":
    #             raise ex
    def get_min(
        self,
        col_name: str,
        field_name: str,
        filter_: dict[str, str] = {},  # pyright: ignore[reportCallInDefaultInitializer]
    ):
        cursor = self.db[col_name].find(filter_).sort(field_name, 1).limit(1)
        return cursor[0][field_name]

    def get_max(self, col_name: str, field_name: str, filter_: dict[str, str] = {}):  # pyright: ignore[reportCallInDefaultInitializer]
        cursor = self.db[col_name].find(filter_).sort(field_name, -1).limit(1)
        return cursor[0][field_name]

    def get_number_of_deposit_wallets(self):
        return self._deposit_wallets_col.estimated_document_count()

    def get_deposit_wallets(self, skip: int, limit: int):
        return (
            self._deposit_wallets_col.find(filter={}).skip(skip=skip).limit(limit=limit)
        )

    def get_user_wallet_from_deposit_wallets(
        self,
        _filter: dict[str, int | str] | None = None,
        projection: dict[str, int] | None = None,
    ) -> Cursor[DepositUserDoc]:
        """
        Get user wallet from deposit wallets collection
        """
        if _filter is None:
            _filter = {}
        if projection is None:
            projection = {"userWallets": 1}
        return self._deposit_users_col.find(filter=_filter, projection=projection)

    def update_deposit_wallets_single_chain(
        self,
        wallets: list[
            dict[str, str | list[str] | int | dict[str, list[dict[str, str]]]]
        ],
    ):
        """For new schema of deposit wallets (with chainId only"""
        try:
            wallet_updates_bulk: list[UpdateOne] = []
            for wallet in wallets:
                wallet["_id"] = f"{wallet['chainId']}_{wallet['address']}"

                # pop all basic information besides data about CEXs
                wallet_base_data = {
                    "_id": wallet.pop("_id"),
                    "chainId": wallet.pop("chainId"),
                    "address": wallet.pop("address"),
                    "lastUpdatedAt": wallet.pop("lastUpdatedAt"),
                }
                tags = wallet.pop("tags")
                # update nested documents
                _mongo_add_to_set_query = {
                    "depositedExchanges": {"$each": wallet["depositedExchanges"]},
                    "tags": {"$each": tags},
                }
                # add update query into bulk
                _filter = {"_id": wallet_base_data["_id"]}
                _update = {
                    "$set": wallet_base_data,
                    "$addToSet": _mongo_add_to_set_query,
                }
                wallet_updates_bulk.append(
                    UpdateOne(filter=_filter, update=_update, upsert=True)
                )

            _ = self._deposit_wallets_col.bulk_write(wallet_updates_bulk)  # pyright: ignore[reportUnknownMemberType]
        except Exception as ex:
            logger.exception(ex)

    def update_subgraphs(
        self,
        data: list[SubgraphDoc],
        chain_name: str,
        radius: int,
        update_time: int,
    ):
        try:
            bulk_updates: list[UpdateOne] = list()
            if update_time:
                for subgraph in data:
                    subgraph["lastUpdatedAt"] = update_time
            for subgraph in data:
                subgraph["_id"] = f"{subgraph['chainId']}_{subgraph['address']}"
                edges: object | list[dict[str, str]] = subgraph.pop("edges", [])
                if isinstance(edges, list) and all(
                    isinstance(edge, dict)
                    and all(
                        isinstance(k, str) and isinstance(v, str)
                        for k, v in edge.items()  # pyright: ignore [reportUnknownVariableType]
                    )
                    for edge in edges  # pyright: ignore [reportUnknownVariableType]
                ):
                    subgraph_new_edges = edges  # pyright: ignore [reportUnknownVariableType]
                else:
                    subgraph_new_edges: list[dict[str, str]] = []
                bulk_updates.append(
                    UpdateOne(
                        filter={"_id": subgraph["_id"]},
                        update={
                            "$set": subgraph,
                            "$addToSet": {"edges": {"$each": subgraph_new_edges}},
                        },
                        upsert=True,
                    )
                )
            if not radius:
                _ = self.db[f"subgraph_{chain_name}"].bulk_write(bulk_updates)  # pyright: ignore [reportUnknownMemberType]
            else:
                _ = self.db[f"subgraph_{chain_name}_{radius}"].bulk_write(bulk_updates)  # pyright: ignore [reportUnknownMemberType]
        except Exception as ex:
            logger.exception(ex)

    # def update_transactions(self, chain_id, data: List[Dict]):
    #     bulk_updates = [
    #         UpdateOne({"_id": datum["_id"]}, {"$set": datum}, upsert=True)
    #         for datum in data
    #     ]
    #     try:
    #         self._db[f"{chain_id}_transactions"].bulk_write(bulk_updates)
    #     except Exception as ex:
    #         logger.exception(ex)

    def update_deposit_users(self, data: list[dict[str, str | list[str] | int]]):
        bulk_updates: list[UpdateOne] = list()
        for datum in data:
            _id = f"{datum['chainId']}_{datum['address']}"
            datum["_id"] = _id
            user_wallets = datum.pop("userWallets", "")
            if user_wallets:
                bulk_updates.append(
                    UpdateOne(
                        filter={"_id": _id},
                        update={
                            "$set": datum,
                            "$addToSet": {"userWallets": {"$each": user_wallets}},
                        },
                        upsert=True,
                    )
                )
        try:
            _ = self._deposit_users_col.bulk_write(bulk_updates)  # pyright: ignore [reportUnknownMemberType]
        except InvalidOperation as ex:
            _message = ex.args[0]
            if _message == NO_OPERATION_TO_EXECUTE:
                logger.debug(_message)
            else:
                logger.exception(f"Error: {_message}")
                write_to_file(ERROR_LOG_FILE, ex)
        except Exception as ex:
            _full_error_log = f"{human_readable_time(int(time.time()))}: {ex} \n"
            write_to_file(ERROR_LOG_FILE, _full_error_log)
            _message = ex.args[0]
            logger.exception(f"Error (Not No operations): {_message}")

    @staticmethod
    def create_update_doc(
        document: DepositUserDoc,
        keep_none: bool = False,
        merge: bool = True,
        shard_key: str | None = None,
    ):
        unset: list[
            dict[str | int, str | ObjectId | list[str] | dict[str, list[str]] | int]
        ] = []
        set_: list[
            DepositUserDoc
            | dict[str | int, str | ObjectId | list[str] | dict[str, list[str]] | int]
        ] = []
        add_to_set: list[
            dict[str | int, str | ObjectId | list[str] | dict[str, list[str]] | int]
        ] = []
        if not keep_none:
            doc: dict[
                str | int, str | list[str] | dict[str, list[str]] | int | None
            ] = flatten_dict(document)
            for key, value in doc.items():
                if value is None:
                    tmp: dict[
                        str | int,
                        str | ObjectId | list[str] | dict[str, list[str]] | int,
                    ] = {
                        "_id": document["_id"],
                        key: "",
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    unset.append(tmp)
                    continue
                if not merge:
                    continue
                if isinstance(value, list):
                    tmp = {
                        "_id": document["_id"],
                        key: {"$each": [i for i in value if i]},
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    add_to_set.append(tmp)
                else:
                    tmp = {"_id": document["_id"], key: value}
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    set_.append(tmp)

        if not merge:
            if keep_none:
                set_.append(document)
            else:
                set_.append(delete_none(document))  # pyright: ignore [reportUnknownArgumentType]

        return unset, set_, add_to_set

    def update_docs(
        self,
        collection_name: str,
        data: list[DepositUserDoc],
        keep_none: bool = False,
        merge: bool = True,
        shard_key: str | None = None,
        flatten: bool = True,
    ):
        """If merge is set to True => sub-dictionaries are merged instead of overwritten"""
        try:
            col = self.db[collection_name]
            # col.insert_many(data, overwrite=True, overwrite_mode='update', keep_none=keep_none, merge=merge)
            bulk_operations: list[UpdateOne] = []
            if not flatten:
                if not shard_key:
                    bulk_operations = [
                        UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True)
                        for item in data
                    ]
                else:
                    bulk_operations = [
                        UpdateOne(
                            {"_id": item["_id"], shard_key: item[shard_key]},
                            {"$set": item},
                            upsert=True,
                        )
                        for item in data
                    ]
                _ = col.bulk_write(bulk_operations)  # pyright: ignore [reportUnknownMemberType]
                return

            for document in data:
                unset, set_, add_to_set = self.create_update_doc(
                    document, keep_none, merge, shard_key
                )
                if not shard_key:
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"]},
                            {
                                "$unset": {
                                    key: value
                                    for key, value in item.items()
                                    if key != "_id"
                                }
                            },
                            upsert=True,
                        )
                        for item in unset
                    ]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"]},
                            {
                                "$set": {
                                    key: value
                                    for key, value in item.items()
                                    if key != "_id"
                                }
                            },
                            upsert=True,
                        )
                        for item in set_
                    ]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"]},
                            {
                                "$addToSet": {
                                    key: value
                                    for key, value in item.items()
                                    if key != "_id"
                                }
                            },
                            upsert=True,
                        )
                        for item in add_to_set
                    ]
                if shard_key:
                    keys = ["_id", shard_key]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"], shard_key: item[shard_key]},
                            {
                                "$unset": {
                                    key: value
                                    for key, value in item.items()
                                    if key not in keys
                                }
                            },
                            upsert=True,
                        )
                        for item in unset
                    ]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"], shard_key: item[shard_key]},
                            {
                                "$set": {
                                    key: value
                                    for key, value in item.items()
                                    if key not in keys
                                }
                            },
                            upsert=True,
                        )
                        for item in set_
                    ]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"], shard_key: item[shard_key]},
                            {
                                "$addToSet": {
                                    key: value
                                    for key, value in item.items()
                                    if key not in keys
                                }
                            },
                            upsert=True,
                        )
                        for item in add_to_set
                    ]
            _ = col.bulk_write(bulk_operations)  # pyright: ignore [reportUnknownMemberType]
        except Exception as ex:
            logger.exception(ex)
