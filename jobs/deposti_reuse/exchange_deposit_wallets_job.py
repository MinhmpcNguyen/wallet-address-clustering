import gc
import time

from multithread_processing.base_job import (  # pyright: ignore [reportMissingTypeStubs]
    BaseJob,
)
from typing_extensions import override

from constants.network_constants import Chains
from databases.clickhouse import ClickHouseCentic
from databases.mongodb import MongoDB
from models.blocks import Blocks
from models.wallet.wallet_deposit_exchange import WalletDepositExchange
from utils.logger_utils import get_logger

logger = get_logger("Exchange Deposit Wallet Job")


class ExchangeDepositWalletsJob(BaseJob):
    """Multithread job to get export wallets that deposit into hot wallets during a time interval (usually 1 day)"""

    def __init__(
        self,
        exporter: MongoDB,
        cassandra: ClickHouseCentic,
        exchange_wallets: dict[str, str],
        burn_wallets: set[str],
        chain_id: str,
        start_timestamp: int,
        end_timestamp: int,
        period: int,
        batch_size: int,
        max_workers: int,
        sources: list[str] | None = None,
    ):
        """
        Args:
            self: Represent the instance of the class
            transfer_event_db: database with transfer events
            blockchain_etl: database with transactions data
            exchange_wallets: dict: Define the wallets that belong to an exchange
            chain_id: Specify the chain that we want to extract data from
            start_timestamp: Set the start_timestamp of the data to be extracted
            end_timestamp: Set the end time of the data to be extracted
            period: Determine the time interval for each worker
            batch_size: Set the number of work for each worker to process parallely
            max_workers: Limit the number of workers that can be used to process the work_iterable
            sources: Specify the source of the data
        """

        self.cassandra: ClickHouseCentic = cassandra
        self.chain_name: str = Chains.names[chain_id]
        self.exporter: MongoDB = exporter

        self.wallets_groupby_exchanges: dict[str, list[str]] = dict()
        self.all_hot_wallets: set[str] = set()
        for wallet_addr, exchange_id in exchange_wallets.items():
            self.wallets_groupby_exchanges.setdefault(exchange_id, []).append(
                wallet_addr
            )
            self.all_hot_wallets.add(wallet_addr)
        self.burn_wallets: set[str] = burn_wallets

        self.chain_id: str = chain_id

        self.start_timestamp: int = start_timestamp
        self.end_timestamp: int = end_timestamp
        self.period: int = period

        if sources is None:
            sources = ["transactions", "token_transfers"]
        self.sources: list[str] = sources
        self._wallets_by_address: dict[str, WalletDepositExchange] = dict()

        work_iterable = range(self.start_timestamp, self.end_timestamp, self.period)
        super().__init__(work_iterable, batch_size, max_workers)  # pyright: ignore [reportUnknownMemberType]

    @override
    def _start(self):
        self._wallets_by_address = dict()  # {address: Wallet object}

    @override
    def _end(self):
        self.batch_executor.shutdown()
        self._export_wallets()

        del self._wallets_by_address
        _ = gc.collect()

    @override
    def _execute_batch(self, works: list[int]):
        start_timestamp = works[0]
        end_timestamp = min(start_timestamp + self.period, self.end_timestamp)
        block_range = Blocks().block_numbers(
            self.chain_id, [start_timestamp, end_timestamp]
        )

        for source in self.sources:
            self._get_wallets_by_address_from_source(
                source, block_range[start_timestamp], block_range[end_timestamp]
            )

    def _get_wallets_by_address_from_source(
        self, source: str, from_block: int, to_block: int
    ):
        for exchange_id, wallet_addresses in self.wallets_groupby_exchanges.items():
            print("from_block", from_block, "to_block", to_block)

            items = []  # Initialize items to avoid being unbound
            if source == "token_transfers":
                items = list(
                    self.cassandra.get_token_transfer_senders_by_receivers(
                        wallet_addresses, from_block, to_block, chain=self.chain_name
                    )
                )
            elif source == "transactions":
                items = list(
                    self.cassandra.get_transaction_senders_by_receivers(
                        wallet_addresses, from_block, to_block, chain=self.chain_name
                    )
                )
            else:
                logger.warning(
                    f"Invalid source: {source}. Supported sources are: {['transactions', 'token_transfers']}"
                )

            from_address_distinct_list: list[str] = list(
                set(str(item._asdict()["from_address"]) for item in items)
            )

            for from_address in from_address_distinct_list:
                if (
                    from_address in self.all_hot_wallets
                    or from_address in self.burn_wallets
                ):
                    continue
                if from_address in self._wallets_by_address:
                    self._wallets_by_address[from_address].add_protocol(
                        protocol_id=exchange_id,
                        address=from_address,
                        chain_id=self.chain_id,
                    )
                else:
                    new_deposit_wallet = WalletDepositExchange(
                        address=from_address, last_updated_at=int(time.time())
                    )
                    self._wallets_by_address[from_address] = new_deposit_wallet
                    self._wallets_by_address[from_address].add_protocol(
                        protocol_id=exchange_id,
                        address=from_address,
                        chain_id=self.chain_id,
                    )

    def _export_wallets(self):
        """Export exchange deposit wallets with tag"""
        wallets = list(self._wallets_by_address.values())
        wallets_data: list[
            dict[str, str | list[str] | int | dict[str, list[dict[str, str]]]]
        ] = [wallet.to_dict_single_chain() for wallet in wallets]
        for datum in wallets_data:
            datum["lastUpdatedAt"] = int(time.time())
        self.exporter.update_deposit_wallets_single_chain(wallets_data)
