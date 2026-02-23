import gc

from multithread_processing.base_job import (  # pyright: ignore [reportMissingTypeStubs]
    BaseJob,
)
from typing_extensions import override

from constants.network_constants import Chains
from databases.clickhouse import CHResultSet, ClickHouseCentic
from databases.mongodb import MongoDB
from models.blocks import Blocks
from utils.logger_utils import get_logger

logger = get_logger("Deposits and Users Job")


class DepositsAndUsersJob(BaseJob):
    """Multithread job to get export wallets that deposit into hot wallets during a time interval (usually 1 day)
    Old version (use MongoDB)
    """

    def __init__(
        self,
        cassandra: ClickHouseCentic,
        mongodb: MongoDB,
        chain_id: str,
        hot_wallets: set[str],
        burn_wallets: set[str],
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int,
        max_workers: int,
        sources: list[str] | None = None,
    ):
        """
        Args:
            self: Represent the instance of the class
            exchange_wallets: dict: Define the wallets that belong to an exchange
            chain_id: Specify the chain that we want to extract data from
            start_timestamp: Set the start_timestamp of the data to be extracted
            end_timestamp: Set the end time of the data to be extracted
            batch_size: Set the number of work for each worker to process parallely
            max_workers: Limit the number of workers that can be used to process the work_iterable
            sources: Specify the source of the data
        """
        self.cassandra: ClickHouseCentic = cassandra
        self._mongodb: MongoDB = mongodb
        if Chains.names.get(chain_id) is None:
            raise ValueError(f"Chain {chain_id} is not supported")
        else:
            self.chain_name: str = str(Chains.names.get(chain_id))
        self.chain_id: str = chain_id
        self._excluded_wallets: set[str] = hot_wallets.union(burn_wallets)

        self.start_timestamp: int = start_timestamp
        self.end_timestamp: int = end_timestamp
        _block_range = Blocks().block_numbers(
            self.chain_id, [self.start_timestamp, self.end_timestamp]
        )
        self.start_block: int = _block_range[self.start_timestamp]
        self.end_block: int = _block_range[self.end_timestamp]

        if sources is None:
            sources = ["transactions", "token_transfers"]
        self.sources: list[str] = sources

        self._number_of_deposit_wallets: int = (
            self._mongodb.get_number_of_deposit_wallets()
        )
        work_iterable: range = range(0, self._number_of_deposit_wallets)
        self.batch_size: int = batch_size
        super().__init__(work_iterable, batch_size, max_workers)  # pyright: ignore [reportUnknownMemberType]

    @override
    def _start(self):
        super()._start()

    @override
    def _end(self):
        super()._end()
        _ = gc.collect()

    @override
    def _execute_batch(self, works: list[int]):
        deposit_wallets_cursor = self._mongodb.get_deposit_wallets(
            skip=works[0], limit=self.batch_size
        )
        deposit_wallets = [doc["address"] for doc in deposit_wallets_cursor]
        for source in self.sources:
            self._get_transfers_to_deposit_wallets(
                source=source, deposit_wallets=deposit_wallets
            )
        logger.info(
            f"Exported data of deposit wallets {works[0]} to {works[-1]} / {len(self.work_iterable)} deposit wallets "  # pyright: ignore [reportUnknownArgumentType,reportUnknownMemberType]
        )

    def _get_transfers_to_deposit_wallets(
        self, source: str, deposit_wallets: list[str]
    ):
        _deposit_wallets: dict[str, set[str]] = dict()
        _user_wallets: dict[str, set[str]] = dict()

        items: CHResultSet | None = None
        if source == "token_transfers":
            items = self.cassandra.get_token_transfer_senders_by_receivers(
                to_addresses=deposit_wallets,
                from_block=self.start_block,
                to_block=self.end_block,
                chain=self.chain_name,
            )
        elif source == "transactions":
            items = self.cassandra.get_transaction_senders_by_receivers(
                to_addresses=deposit_wallets,
                from_block=self.start_block,
                to_block=self.end_block,
                chain=self.chain_name,
            )
        else:
            logger.warning(
                f"Invalid source: {source}. Supported sources are: {['transactions', 'token_transfers']}"
            )

        if items is None:
            logger.error("No items retrieved due to invalid source.")
            return

        for item_raw in items:
            item = item_raw._asdict()
            from_address = item["from_address"]
            to_address = item["to_address"]

            # filter hot wallets & burn wallets
            if {from_address, to_address}.intersection(self._excluded_wallets):
                continue

            # user wallet
            if from_address in _user_wallets:
                _user_wallets[from_address].add(to_address)
            else:
                _user_wallets[from_address] = {to_address}

            # deposit wallet
            if to_address in _deposit_wallets:
                _deposit_wallets[to_address].add(from_address)
            else:
                _deposit_wallets[to_address] = {from_address}

        self._export_deposit_users(_deposit_wallets)

    def _export_deposit_users(self, deposit_wallets: dict[str, set[str]]):
        exported_data: list[dict[str, str | list[str] | int]] = [
            {
                "address": address,
                "chainId": self.chain_id,
                "userWallets": list(wallet_data),
                "updating": 1,
            }
            for address, wallet_data in deposit_wallets.items()
        ]
        self._mongodb.update_deposit_users(exported_data)

    # def _export_user_deposits(self, user_wallets: Dict[str, Set]):
    #     exported_data = [
    #         {
    #             "address": address,
    #             "chainId": self.chain_id,
    #             "depositWallets": list(wallet_data),
    #         }
    #         for address, wallet_data in user_wallets.items()
    #     ]
    #     self._mongodb.update_user_deposits(exported_data)
