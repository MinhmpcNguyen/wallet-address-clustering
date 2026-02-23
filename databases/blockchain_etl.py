# from typing import Dict, List

# from pymongo import MongoClient

# from config import BlockchainETLConfig
# from constants.blockchain_etl_constants import BlockchainETLCollections
# from utils.logger_utils import get_logger

# logger = get_logger("Blockchain ETL")


# class BlockchainETL:
#     def __init__(self, connection_url=None, db_prefix=""):
#         self._conn = None
#         if not connection_url:
#             connection_url = BlockchainETLConfig.CONNECTION_URL

#         self.connection_url = connection_url.split("@")[-1]
#         self.connection = MongoClient(connection_url)
#         if db_prefix:
#             db_name = db_prefix + "_" + BlockchainETLConfig.DATABASE
#         else:
#             db_name = BlockchainETLConfig.DATABASE

#         self.mongo_db = self.connection[db_name]

#         self.block_collection = self.mongo_db[BlockchainETLCollections.blocks]
#         self.transaction_collection = self.mongo_db[
#             BlockchainETLCollections.transactions
#         ]
#         self.collector_collection = self.mongo_db[BlockchainETLCollections.collectors]

#     def get_transactions_relate_to_list_addresses(
#         self, addresses, from_block, to_block
#     ):
#         filter_ = {
#             "block_number": {"$gte": from_block, "$lt": to_block},
#             "$or": [
#                 {"from_address": {"$in": addresses}},
#                 {"to_address": {"$in": addresses}},
#             ],
#         }
#         cursor = self.transaction_collection.find(filter_).batch_size(1000)
#         return cursor

#     # @sync_log_time_exe(tag=TimeExeTag.database)
#     def get_transactions_to_addresses(
#         self, to_addresses, from_block, to_block, projection=["from_address"]
#     ):
#         """For getting transactions to a list addresses (such as hot wallets)"""
#         filter_ = {
#             "$and": [
#                 {"block_number": {"$gte": from_block, "$lte": to_block}},
#                 {"to_address": {"$in": [address.lower() for address in to_addresses]}},
#             ]
#         }
#         cursor = self.transaction_collection.find(
#             filter_, projection=projection
#         ).batch_size(10000)
#         return cursor

#     def get_native_transfer_txs(
#         self, from_block: int, to_block: int, projection: List = None
#     ):
#         # TODO: if to_address is None => use receipt_contract_address
#         filter_ = {
#             "$and": [
#                 {"block_number": {"$gte": from_block, "$lte": to_block}},
#                 {"input": "0x"},
#                 {"value": {"$ne": "0"}},
#                 {"receipt_status": 1},
#             ]
#         }
#         if not projection:
#             projection = [
#                 "from_address",
#                 "to_address",
#                 "value",
#                 "block_timestamp",
#                 "hash",
#                 "receipt_contract_address",
#             ]
#         return self.transaction_collection.find(
#             filter_, projection=projection
#         ).batch_size(1000)

#     def get_blocks_in_range(self, start_block, end_block, projection: List = None):
#         filter_ = {"number": {"$gte": start_block, "$lte": end_block}}
#         if not projection:
#             cursor = self.block_collection.find(filter_).batch_size(1000)
#         else:
#             cursor = self.block_collection.find(
#                 filter=filter_, projection=projection
#             ).batch_size(1000)
#         return cursor

#     ########################################
#     # Return direct data instead of cursor #
#     ########################################

#     def get_block_number_to_timestamp(
#         self, start_block: str, end_block: str
#     ) -> Dict[int, int]:
#         cursor = self.get_blocks_in_range(
#             start_block=start_block,
#             end_block=end_block,
#             projection=["number", "timestamp"],
#         )
#         return {block["number"]: block["timestamp"] for block in cursor}
