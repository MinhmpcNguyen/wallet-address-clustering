import os
import sys

from multithread_processing.base_job import BaseJob

sys.path.append(os.path.dirname(sys.path[0]))

from constants.network_constants import Chains
from databases.blockchain_etl import BlockchainETL
from databases.mongodb import MongoDB
from utils.logger_utils import get_logger

DECIMALS = 10**18
logger = get_logger("Transactions Retriever")


class TransactionsExporter(BaseJob):
    def __init__(
        self,
        wallets_list: list,
        start_block: int,
        end_block: int,
        chain_id: str,
        max_workers: int = 16,
        batch_size: int = 1000,
    ):
        """
        Export a selected set of transactions, for easy & quick query compared to querying from Mongo Main
        """
        self.wallets_list = wallets_list
        self.end_block = end_block
        self.start_block = start_block

        self.mongodb = MongoDB()

        self.chain_id = chain_id
        _db_prefix = ""
        if chain_id != "0x38":
            _db_prefix = Chains.names[chain_id]
        self.blockchain_etl = BlockchainETL(db_prefix=_db_prefix)

        _work_iterable = list(range(self.end_block, self.start_block, -1))
        _work_iterable.append(0)
        super().__init__(
            work_iterable=_work_iterable, max_workers=max_workers, batch_size=batch_size
        )

    def _execute_batch(self, works):
        to_block = works[0]
        from_block = works[-1]
        retrieved_transactions = list(
            self.blockchain_etl.get_transactions_relate_to_list_addresses(
                addresses=self.wallets_list, from_block=from_block, to_block=to_block
            )
        )
        self.mongodb.update_transactions(
            chain_id=self.chain_id, data=retrieved_transactions
        )

        progress = (self.end_block - from_block) / self.end_block
        print(
            f"Export from {from_block} to {to_block} / {self.end_block}. Estimated progress: {progress * 100} %"
        )
