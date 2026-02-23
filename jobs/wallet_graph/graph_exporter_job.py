import gc
import json
import time
from collections.abc import Iterator
from logging import Logger
from math import ceil

from arango.exceptions import ArangoServerError
from arango.result import Result
from arango.typings import Json
from multithread_processing.base_job import (  # pyright: ignore [reportMissingTypeStubs]
    BaseJob,
)
from typing_extensions import override

from config import ArangoDBConfig
from constants.network_constants import NATIVE_TOKENS, NATIVE_TOKENS_DECIMALS, Chains
from constants.time_constants import TimeConstants
from databases.arangodb_klg import AddressGraphClient
from databases.clickhouse import ClickHouseCentic
from databases.mongodb_entity import MongoDBEntity
from models.blocks import Blocks
from models.graph.transfer import Transfer
from models.graph.transfer_logs import Edge
from schemas.graph_schema import TransferToGraphSchema
from service.coingecko import get_historical_prices
from utils.list_dict_utils import chunks
from utils.logger_utils import get_logger
from utils.time_utils import (
    human_readable_time,
    round_timestamp,
    round_timestamp_for_log,
)

logger = get_logger("Graph Exporter Job")


class GraphExporterJob(BaseJob):
    """Job to fill transfer & transaction graph, in a specific timestamps range"""

    def __init__(
        self,
        start_timestamp: int,
        end_timestamp: int,
        # Databases
        cassandra: ClickHouseCentic,
        mongo_klg: MongoDBEntity,
        # arangodb: Arango,
        chain_id: str,
        hot_wallets: set[str],
        burn_wallets: set[str],
        batch_size: int = 100,
        max_workers: int = 8,
        sources: list[str] | None = None,
    ):
        """
        Args:
            self: Represent the instance of the class
            start_timestamp: Set the start_timestamp of the data to be extracted
            end_timestamp: Set the end time of the data to be extracted
            cassandra: database with transactions and transfer events
            mongo_klg: database with smart contract a.k.a token data
            chain_id: Specify the chain that we want to extract data from
            hot_wallets: Set of hot wallet's addresses
            burn_wallets: Set of burn wallet's addresses
            batch_size: Determine the number of works for each worker each batch
            max_wokers: Set the number of work for each worker to process parallely
            sources: Specify the source of the data
        """
        self.mongo_klg: MongoDBEntity = mongo_klg
        self.cassandra: ClickHouseCentic = cassandra
        self.chain_id: str = chain_id
        if Chains.names.get(chain_id) is None:
            raise ValueError(f"Chain {chain_id} is not supported")
        else:
            self.chain_name: str = str(Chains.names.get(chain_id))
        self.native_token_address: str = NATIVE_TOKENS[self.chain_id]

        self.arango: _Arango = _Arango(
            ArangoDBConfig.CONNECTION_URL, prefix=self.chain_name
        )

        if not sources:
            sources = ["transactions", "token_transfers"]
        self.sources: list[str] = sources

        self.hot_wallets: set[str] = hot_wallets
        self.burn_wallets: set[str] = burn_wallets

        self.top_tokens: set[str] = set()

        # Check if getting price from Coingecko
        self.if_coingecko: bool = True
        self.coingecko_ids: dict[str, str] = dict()
        self.already_called_api_addresses: set[str] = set()

        self.start_timestamp: int = start_timestamp
        self.end_timestamp: int = end_timestamp

        self.tokens_price_logs: dict[str, dict[int, float]] = dict()

        self.batch_size: int = batch_size
        self.max_workers: int = max_workers
        self.work_iterable: list[int] = list(
            range(self.start_timestamp, self.end_timestamp)
        )
        super().__init__(  # pyright: ignore [reportUnknownMemberType]
            work_iterable=self.work_iterable,
            batch_size=batch_size,
            max_workers=max_workers,
        )

    @override
    def _start(self):
        if self.if_coingecko:
            self.coingecko_ids = self.mongo_klg.get_coingecko_ids(
                chain_id=self.chain_id
            )
        with open(f"artifacts/tokens/top_tokens_{self.chain_id}.json", "r") as f:
            _top_tokens_doc = json.load(f)[0]
        self.top_tokens = {token["address"] for token in _top_tokens_doc["tokens"]}

    @override
    def _execute_batch(self, works: list[int]) -> None:
        _time0 = time.time()

        _start_timestamp: int = works[0]
        _end_timestamp: int = works[-1]
        # logger.info(f"Start processing on chain {self.chain_name} from "
        #             f"{human_readable_time(_start_timestamp)} to {human_readable_time(_end_timestamp)}...")
        _timestamp_block_mappings: dict[int, int] = Blocks().block_numbers(
            chain_id=self.chain_id, timestamps=[_start_timestamp, _end_timestamp]
        )
        from_block = _timestamp_block_mappings[_start_timestamp]
        to_block = _timestamp_block_mappings[_end_timestamp]

        print("from_block:", from_block, "to_block:", to_block)

        _block_timestamp_mappings: dict[int, int] = (
            self.cassandra.get_block_number_to_timestamp(
                from_block=from_block, to_block=to_block, chain=self.chain_name
            )
        )

        transfers: list[Transfer] = self._get_transfers_from_sources(
            from_block=from_block,
            to_block=to_block,
            block_timestamp_mappings=_block_timestamp_mappings,
        )
        # self._update_non_human_addresses(transfers=transfers)
        self._update_token_price_logs(
            token_addresses=list(set(transfer.coin_addr for transfer in transfers))
        )

        addresses: dict[str, dict[str, str | int | dict[str, int]]] = (
            dict()
        )  # addresses: dict = {addr: {chainId:, address:, lastTransferAt:}}
        transfers_logs: dict[str, Edge] = (
            dict()
        )  # transfers_log: dict = {chainId_fromAddr_toAddr: Transfer}
        _time_iterate_transfer = float(time.time())
        for _i, transfer in enumerate(transfers):
            if self._not_to_be_uploaded_to_graph(transfer):
                continue

            # Nodes
            new_from_address: dict[str, str | int | dict[str, int]] = {
                "chainId": self.chain_id,
                "address": transfer.from_addr,
                "lastTransferAt": transfer.timestamp,
            }
            new_to_address: dict[str, str | int | dict[str, int]] = {
                "chainId": self.chain_id,
                "address": transfer.to_addr,
                "lastTransferAt": transfer.timestamp,
            }
            if transfer.to_addr in self.hot_wallets:
                new_to_address["wallet"] = {"hotWallet": 1}
                new_from_address["wallet"] = {"depositWallet": 1}

            addresses.update(
                {
                    self.chain_id + "_" + transfer.from_addr: new_from_address,  # noqa: E501
                    self.chain_id + "_" + transfer.to_addr: new_to_address,  # noqa: E501
                }
            )

            # Edges
            _rounded_block_timestamp = round_timestamp(
                transfer.timestamp, round_time=TimeConstants.A_DAY
            )
            _token_price = self._get_token_price(
                token_address=transfer.coin_addr, timestamp=_rounded_block_timestamp
            )
            if _token_price:
                transfer_value_in_usd = transfer.amount * _token_price
            else:
                transfer_value_in_usd = None
            transfer_id = f"{self.chain_id}_{transfer.from_addr}_{transfer.to_addr}"
            if transfer_id not in transfers_logs:
                transfers_logs[transfer_id] = Edge(
                    chain_id=self.chain_id,
                    from_address=transfer.from_addr,
                    to_address=transfer.to_addr,
                )
            transfers_logs[transfer_id].add_transfer(
                transfer=transfer, value_in_usd=transfer_value_in_usd
            )

            # print(transfers_logs)
            # if not (_i % 10000):
            #     logger.info(f"process transfer {_i} / {len(transfers)} "
            #                 f"from {human_readable_time(_start_timestamp)} to {human_readable_time(_end_timestamp)}, "
            #                 f"took {float(time.time()) - _time_iterate_transfer:.2f}s")

        self._export_addresses_to_graph(addresses)
        self._export_transfers_to_graph(transfers_logs)

        logger.info(
            f"Exported {len(addresses)} vertices and {len(transfers_logs)} edges from {human_readable_time(_start_timestamp)} to {human_readable_time(_end_timestamp)} to graph. Took {time.time() - _time0:.2f}s"
        )

    def _get_transfers_from_sources(
        self, from_block: int, to_block: int, block_timestamp_mappings: dict[int, int]
    ) -> list[Transfer]:
        transfers: list[Transfer] = list()
        for source in self.sources:
            if source == "token_transfers":
                transfers.extend(
                    self._get_transfers(
                        from_block=from_block,
                        to_block=to_block,
                        block_timestamp_mappings=block_timestamp_mappings,
                    )
                )
            elif source == "transactions":
                transfers.extend(
                    self._get_transactions(from_block=from_block, to_block=to_block)
                )
        return transfers

    def _get_transfers(
        self, from_block: int, to_block: int, block_timestamp_mappings: dict[int, int]
    ) -> list[Transfer]:
        transfers: list[Transfer] = list()
        transfers_data = self.cassandra.get_event_transfers(
            from_block=from_block, to_block=to_block, chain=self.chain_name
        )
        # token_addresses = [transfer['contract_address'] for transfer in transfers_data]
        for transfer_datum_raw in transfers_data:
            transfer_datum = transfer_datum_raw._asdict()
            transfer_timestamp = block_timestamp_mappings[
                transfer_datum["block_number"]
            ]
            transfer = Transfer(
                chain_id=self.chain_id,
                from_addr=transfer_datum["from_address"],
                to_addr=transfer_datum["to_address"],
                coin_addr=transfer_datum["contract_address"],
                amount=transfer_datum["value"],
                timestamp=transfer_timestamp,
            )
            transfers.append(transfer)
        return transfers

    def _get_transactions(self, from_block: int, to_block: int) -> list[Transfer]:
        transfers: list[Transfer] = list()
        transactions_data = self.cassandra.get_native_transfer_txs(
            from_block, to_block, chain=self.chain_name
        )
        for tx_datum_raw in transactions_data:
            tx_datum = tx_datum_raw._asdict()
            transfer = Transfer(
                chain_id=self.chain_id,
                from_addr=tx_datum["from_address"],
                to_addr=tx_datum["to_address"]
                if tx_datum["to_address"]
                else tx_datum["receipt_contract_address"],
                coin_addr=self.native_token_address,
                amount=float(tx_datum["value"]) / NATIVE_TOKENS_DECIMALS[self.chain_id],
                timestamp=tx_datum["block_timestamp"],
            )
            transfers.append(transfer)
        return transfers

    def _export_addresses_to_graph(
        self, vertices: dict[str, dict[str, str | int | dict[str, int]]]
    ):
        for key, value in vertices.items():
            value.update({"_key": key})
        exported_data: list[dict[str, str | int | dict[str, int]]] = list(
            vertices.values()
        )
        _ = self.arango.update_addresses(exported_data)

    def _export_transfers_to_graph(self, edges: dict[str, Edge]):
        exported_data: list[TransferToGraphSchema] = list()
        for key, transfer in edges.items():
            _chain_id, _from, _to = key.split("_")
            exported_data.append(
                {
                    "_key": key,
                    "_from": f"{self.arango.addresses_col_name}/{_chain_id}_{_from}",
                    "_to": f"{self.arango.addresses_col_name}/{_chain_id}_{_to}",
                    "tokenTransferLogs": transfer.get_transfer_logs(),
                }
            )
        _ = self.arango.update_transfers(exported_data)

    def _update_token_price_logs(self, token_addresses: list[str]) -> None:
        tokens_to_update = [
            token_addr
            for token_addr in token_addresses
            if token_addr not in self.tokens_price_logs
        ]
        if tokens_to_update:
            _batches_tokens: Iterator[list[str]] = chunks(tokens_to_update, size=1000)
            for _tokens in _batches_tokens:
                try:
                    cursor = self.mongo_klg.get_price_change_logs(
                        chain_id=self.chain_id, token_addresses=_tokens
                    )
                    for datum in cursor:
                        _token_addr = datum["_id"].split("_")[1]
                        # logger.info(datum["priceChangeLogs"])
                        _token_price_log = {
                            int(ts): float(str(price))
                            for ts, price in datum["priceChangeLogs"].items()
                        }
                        self.tokens_price_logs[_token_addr] = round_timestamp_for_log(
                            _token_price_log, round_time=TimeConstants.A_DAY
                        )
                except Exception as ex:
                    logger.exception(ex)
                    raise ex

    # def _update_non_human_addresses(self, transfers: list[Transfer]):
    #     _from_address_keys = [f'{self.chain_id}_{t.from_addr}' for t in transfers]
    #     _non_human_addresses = self.arango.get_non_human_addresses(_from_address_keys)
    #     self.non_human_addresses.update(set(_non_human_addresses))

    def _not_to_be_uploaded_to_graph(self, transfer: Transfer) -> bool:
        """Some transfers are not to be uploaded to Arango:
        - Transfers from hot wallets
        - Transfers with burn wallets
        """
        if (
            # transfer.from_addr in self.non_human_addresses or
            transfer.from_addr in self.hot_wallets
            or transfer.from_addr in self.burn_wallets
            or transfer.to_addr in self.burn_wallets
        ):
            return True
        return False

    def _get_token_price(self, token_address: str, timestamp: int) -> float | None:
        """
        Get token price of a token by calling coingecko API
        """
        if token_address in self.tokens_price_logs:
            while True:
                _token_price = self.tokens_price_logs[token_address].get(
                    timestamp, None
                )
                if (
                    (_token_price is not None)
                    or (token_address in self.already_called_api_addresses)
                    or (token_address not in self.top_tokens)
                    or (not self.if_coingecko)
                ):
                    return _token_price
                token_coingecko_id = self.coingecko_ids.get(token_address)
                number_of_days = (
                    ceil((time.time() - self.start_timestamp) / TimeConstants.A_DAY) + 1
                )
                token_prices_history = get_historical_prices(
                    coin_id=token_coingecko_id, days=number_of_days
                )
                self.tokens_price_logs[token_address].update(
                    round_timestamp_for_log(
                        token_prices_history, round_time=TimeConstants.A_DAY
                    )
                )
                if self.if_coingecko:
                    self.already_called_api_addresses.add(token_address)

        return None

    @override
    def _end(self):
        super()._end()

        del self.coingecko_ids
        del self.tokens_price_logs
        _ = gc.collect()


class _Arango(AddressGraphClient):
    def __init__(self, connection_url: str, prefix: str):
        super().__init__(connection_url=connection_url, prefix=prefix)
        self.logger: Logger = get_logger("Graph Exporter Job")

    def update_addresses(
        self, data: list[dict[str, str | int | dict[str, int]]]
    ) -> Result[bool | list[Json | ArangoServerError]]:
        """Update addresses collection. Data must have _key field"""
        try:
            result: Result[bool | list[Json | ArangoServerError]] = (
                self._addresses_col.insert_many(
                    data, sync=True, overwrite_mode="update"
                )
            )
            return result
        except Exception as ex:
            self.logger.exception(ex)
            raise ex

    def update_transfers(
        self, data: list[TransferToGraphSchema]
    ) -> Result[bool | list[Json | ArangoServerError]]:
        """Update transfers collection
        Args:
            data: List[Dict]: Pass in a list of dictionaries. Dicts must have '_key' and '_from' and '_to' field
        """
        try:
            list_transfer = [{**transfer} for transfer in data]
            result: Result[bool | list[Json | ArangoServerError]] = (
                self._transfers_col.insert_many(
                    list_transfer, sync=True, overwrite=True, merge=True, silent=True
                )
            )
            return result
        except Exception as ex:
            self.logger.exception(ex)
            raise ex
