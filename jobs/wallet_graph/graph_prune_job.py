import gc
import os
import time

from arango import AQLQueryExecuteError, AQLQueryKillError
from arango.result import Result
from cli_scheduler.scheduler_job import (  # pyright: ignore [reportMissingTypeStubs]
    SchedulerJob,
)
from multithread_processing.base_job import (  # pyright: ignore [reportMissingTypeStubs]
    BaseJob,
)
from requests.exceptions import ReadTimeout
from typing_extensions import override

from config import ArangoDBConfig
from constants.network_constants import Chains, Networks
from constants.time_constants import TimeConstants
from databases.arangodb_klg import AddressGraphClient
from models.graph.transfer_logs import Edge
from schemas.graph_schema import TransferToGraphSchema
from utils.file_utils import (
    init_last_synced_file,
    read_last_synced_file,
    write_last_synced_file,
)
from utils.filter_contract import check_if_contracts
from utils.logger_utils import get_logger
from utils.time_utils import human_readable_time, round_timestamp

logger = get_logger("Graph Prune Job")

BOT_THRESHOLD = 900  # if a wallet with numberSent > BOT_THRESHOLD, then marked as bot
# BOT_THRESHOLD = 600  # if a wallet with numberSent > BOT_THRESHOLD, then marked as bot


class _Arango(AddressGraphClient):
    def __init__(self, prefix: str, connection_url: str):
        super().__init__(connection_url=connection_url, prefix=prefix)
        self._filter: str = ""
        # self._vertex_ids_to_prune: list[str] = list()

    ######################
    #  Vertices Prune    #
    ######################
    def get_number_of_vertices_to_prune(self, timestamp: int) -> int:
        _query = f"""FOR w IN {self.addresses_col_name}
        FILTER w.lastTransferAt < {timestamp}
        COLLECT WITH COUNT INTO n
        RETURN n"""
        # list_of_vertice = list(self.query(_query, ttl=TimeConstants.MINUTES_15))[0]
        # return int(list_of_vertice)
        return list(self.db.aql.execute(_query, max_runtime=TimeConstants.MINUTES_15))[  # pyright: ignore [reportArgumentType]
            0
        ]  # Have to or else query error self._db.aql.execute

    def get_vertex_keys_to_prune(
        self, timestamp: int, batch_size: int = 10000
    ) -> list[str]:
        _query = f"""FOR w IN {self.addresses_col_name}
        FILTER w.lastTransferAt < {timestamp}
        LIMIT {batch_size}
        RETURN w._key"""
        # return list(str(self.query(_query)))
        return list(self.db.aql.execute(_query))  # pyright: ignore [reportArgumentType]

    def remove_vertices(self, vertex_keys: list[str]):
        _query_prune = f"""FOR k IN {vertex_keys}
            LET id = CONCAT('{self.addresses_col_name}/', k)
            LET edgeKeys = (
                FOR v, e IN 1..1 ANY id GRAPH '{self.transfers_graph_name}'
                REMOVE e._key IN {self.transfers_col_name}
                OPTIONS {{ ignoreErrors: true }}
                )
            REMOVE k IN {self.addresses_col_name}"""
        # _ = self.query(_query_prune)
        _ = self.db.aql.execute(_query_prune)

    #################
    #  Edges Prune  #
    #################
    def get_number_of_edges_to_prune(self, timestamp: int) -> int:
        _query = f"""FOR e IN {self.transfers_col_name}
        FILTER NOT e.oldestTransferAt OR e.oldestTransferAt < {timestamp}
        COLLECT WITH COUNT INTO n
        RETURN n"""
        # return list(self._db.aql.execute(_query, max_runtime=TimeConstants.A_MINUTE))[0]
        # return list(self._db.aql.execute(_query, ttl=TimeConstants.A_MINUTE))[0]
        # _cursor = list(self.query(_query, ttl=30))[0]
        # return int(_cursor)
        _cursor = self.db.aql.execute(_query, ttl=30, max_runtime=40)  # pyright: ignore [reportArgumentType]
        return list(_cursor)[0]  # pyright: ignore [reportArgumentType]

    def get_edges_to_prune(self, timestamp: int, batch_size: int = 10000) -> Result:  # pyright: ignore [reportMissingTypeArgument,reportUnknownParameterType]
        # Iterator[Mapping[str | int, Any]]:
        _query = f"""FOR e IN {self.transfers_col_name}
        FILTER NOT e.oldestTransferAt OR e.oldestTransferAt < {timestamp}
        LIMIT {batch_size}
        RETURN e"""
        # return self.query(query=_query, batch_size=batch_size)
        return self.db.aql.execute(_query, batch_size=batch_size, count=True)

    def update_edges(self, data: list[TransferToGraphSchema]):
        """Update transfers collection
        Args:
            data: List[Dict]: Pass in a list of dictionaries. Dicts must have '_key' field
        """
        for datum in data:  # validate
            if "_key" not in datum.keys():
                raise ValueError(f"Error at update edge: {datum}: must contains _key")

        try:
            list_data = [{**datum} for datum in data]  # convert to list of dict
            _ = self._transfers_col.update_many(
                documents=list_data,
                merge=False,
                keep_none=False,
                sync=True,
                silent=True,
            )
        except Exception as ex:
            logger.exception(ex)
            raise ex

    def remove_edges(self, edge_keys: list[str]):
        _query = f"""FOR k IN {edge_keys}
        REMOVE {{_key: k}} IN {self.transfers_col_name}"""
        # _ = self.query(_query)
        _ = self.db.aql.execute(_query)

    ######################
    #  Vertices Update   #
    ######################
    def set_filter(self, last_updated_at_threshold: int):
        # self._filter = f"""FILTER NOT (w.wallet.hotWallet AND w.wallet.bot AND w.wallet.contract)
        #             AND (w.lastUpdatedAt < w.lastTransferAt AND w.lastUpdatedAt < {last_updated_time_threshold}
        #                  OR NOT w.lastUpdatedAt)\n"""
        self._filter = f"""FILTER NOT (w.wallet.hotWallet OR w.wallet.bot OR w.wallet.contract)
            AND (w.lastUpdatedAt < {last_updated_at_threshold} 
                OR NOT w.lastUpdatedAt)\n"""

    def get_number_of_vertices_to_update(self) -> int:
        # self.set_filter(last_updated_time_threshold=filter_timestamp)
        _query = (
            f"""FOR w IN {self.addresses_col_name} \n"""
            + self._filter
            + """COLLECT WITH COUNT INTO n
                  RETURN n"""
        )
        # data = list(self.query(_query))[0]
        # return int(data)
        data = list(self.db.aql.execute(_query))[0]  # pyright: ignore [reportArgumentType]
        return data

    def get_vertex_keys_to_update(self, limit: int = 10000) -> list[str]:
        _query = (
            f"""FOR w IN {self.addresses_col_name}\n"""
            + self._filter
            + f"""LIMIT {limit}\n"""
            + """RETURN w._key"""
        )
        # data = list(str(self.query(_query)))
        # return data
        data = list(self.db.aql.execute(_query))  # pyright: ignore [reportArgumentType]
        return data

    def update_number_sent_received(self, vertex_keys: list[str]) -> Result:  # pyright: ignore [reportMissingTypeArgument,reportUnknownParameterType]
        _time = int(time.time())
        _query_update_sent_received = f"""FOR k IN {vertex_keys}
                LET id = CONCAT('{self.addresses_col_name}/', k)
                LET n_to = (FOR v IN 1..1 OUTBOUND id
                            GRAPH {self.transfers_graph_name}
                            COLLECT WITH COUNT INTO n
                            RETURN n)
                LET n_from = (FOR v IN 1..1 INBOUND id
                              GRAPH {self.transfers_graph_name}
                              COLLECT WITH COUNT INTO n
                              RETURN n)
                UPDATE {{_key: k, numberSent: n_to[0], numberReceived: n_from[0], lastUpdatedAt:{_time} }}
                    IN {self.addresses_col_name}
                LET updated = NEW
                RETURN updated
                """
        # return self.query(query=_query_update_sent_received)
        return self.db.aql.execute(_query_update_sent_received)

    def set_tag_for_wallet_vertex(self, keys: list[str], tag: str):
        _query_update_bots = f"""FOR k IN {keys} 
            UPDATE {{_key: k, wallet: {{{tag}: true}}}} 
            IN {self.addresses_col_name} 
            OPTIONS {{ mergeObjects: true }}"""
        # _ = self.query(_query_update_bots)
        _ = self.db.aql.execute(_query_update_bots)

    def set_contract_tag_for_vertex(self, keys: list[str]):
        _query_update_bots = f"""FOR k IN {keys} 
            UPDATE {{_key: k, contract: true, wallet: false}} IN {self.addresses_col_name}
            OPTIONS {{mergeObjects: true }}"""
        # _ = self.query(_query_update_bots)
        _ = self.db.aql.execute(_query_update_bots)

    def set_number_sent_to_null(self, vertex_keys: list[str]):
        _query_set_number_sent = f"""FOR k in {vertex_keys}
            UPDATE {{ _key: k, numberSent: null, lastUpdatedAt: {int(time.time())} }} IN {self.addresses_col_name}
            OPTIONS {{ ignoreErrors: true}}"""
        # _ = self.query(_query_set_number_sent)
        _ = self.db.aql.execute(_query_set_number_sent)

    def remove_all_dangling_vertices(self):
        _query = f"""FOR w IN {self.addresses_col_name}
            FILTER w.numberSent == 0 AND w.numberReceived == 0
            REMOVE {{_key: w._key}} IN {self.addresses_col_name}
            OPTIONS {{ ignoreErrors: true}}"""
        # _ = self.query(_query)
        _ = self.db.aql.execute(_query)


class GraphPruneJob(SchedulerJob):
    """Add numberSent, numberReceived and lastUpdatedAt for addresses, mark bots/contracts,
    then prune transfers from bots/contracts
    """

    def __init__(
        self,
        chain_id: str,
        batch_size_query: int = 1000,
        batch_size_thread: int = 10,
        max_workers: int = 1,
        timespan: int = 100,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        interval: int = TimeConstants.A_DAY,
        delay: int = 0,
        run_now: bool = True,
        last_synced_file: str = "",
        # arango_conn: str | None = None,
    ):
        self.chain_id: str = chain_id
        if Chains.names.get(chain_id) is None:
            raise ValueError(f"Chain {chain_id} is not supported")
        else:
            self.chain_name: str = str(Chains.names.get(chain_id))
        self.arango: _Arango = _Arango(
            prefix=self.chain_name, connection_url=ArangoDBConfig.CONNECTION_URL
        )
        self.timespan: int = timespan
        self.interval: int = interval
        self.delay: int = delay
        self.start_timestamp: int = (
            start_timestamp if start_timestamp is not None else 0
        )
        scheduler = f"^{run_now}@{interval}/{delay}${end_timestamp}#true"
        super().__init__(scheduler)  # pyright: ignore [reportUnknownMemberType]
        self.last_synced_file: str = last_synced_file

        self.batch_size_query: int = batch_size_query
        self.batch_size_thread: int = batch_size_thread
        self.max_workers: int = max_workers
        self.timestamp_to_prune: int = 0  # Initialize with a default value

    @override
    def _pre_start(self):
        if (self.start_timestamp) or (not os.path.isfile(self.last_synced_file)):
            _DEFAULT_START_TIME = round_timestamp(
                int(time.time()), round_time=self.interval
            )
            init_last_synced_file(
                self.start_timestamp or _DEFAULT_START_TIME, self.last_synced_file
            )
        self.start_timestamp = read_last_synced_file(self.last_synced_file)

    @override
    def _start(self):
        self.next_synced_timestamp: int = (
            round_timestamp(self.start_timestamp + self.interval, self.interval)
            + self.delay
        )
        logger.info(
            f"Start execute from {human_readable_time(self.start_timestamp)} to {human_readable_time(self.next_synced_timestamp)}"
        )

        self.timestamp_to_prune = (
            round_timestamp(timestamp=int(time.time())) - self.interval * self.timespan
        )
        # self.timestamp_to_prune = 1702973095

    @override
    def _execute(self):
        # Prune vertices
        logger.info("Start pruning vertices...")
        self._prune_vertices()

        # Prune edges
        logger.info("Start pruning edges...")
        self._prune_edges()

        # Update vertices after pruning
        logger.info("Start update vertices after pruning...")
        self._update_vertices()

    def _prune_vertices(self):
        """Prune outdated vertices (with lastUpdatedAt < timestamp_to_prune)"""
        _count = 0
        logger.info("Getting number of vertices to prune...")
        try:
            n_vertices_to_prune = self.arango.get_number_of_vertices_to_prune(
                timestamp=self.timestamp_to_prune
            )
        except (AQLQueryKillError, AQLQueryExecuteError):
            logger.warning(
                "Getting number of edges to prune took too long. Getting total number of vertices instead"
            )
            n_vertices_to_prune = self.arango.get_number_of_addresses()

        logger.info(f"Number of vertices to prune: {n_vertices_to_prune}")
        while True:
            vertex_keys_to_prune = self.arango.get_vertex_keys_to_prune(
                timestamp=self.timestamp_to_prune, batch_size=self.batch_size_query
            )
            if not vertex_keys_to_prune:
                break
            _job = _GraphVerticesPruner(
                chain_id=self.chain_id,
                vertex_keys=vertex_keys_to_prune,
                batch_size=self.batch_size_thread,
                max_workers=self.max_workers,
                arango=self.arango,
                timestamp_to_prune=self.timestamp_to_prune,
            )
            _job.run()
            _count += self.batch_size_query
            del _job
            logger.info(f"Vertices Pruning: {_count} / {n_vertices_to_prune} edges.")

        _ = gc.collect()
        logger.info(f"Finished pruning {_count} / {n_vertices_to_prune} vertices")

    def _prune_edges(self):
        """Prune edges with oldestTransferAt < timestamp_to_prune"""
        _count = 0
        logger.info("Getting number of edges to prune...")

        try:
            n_edges_to_prune = self.arango.get_number_of_edges_to_prune(
                timestamp=self.timestamp_to_prune
            )
        except (AQLQueryKillError, AQLQueryExecuteError):
            logger.warning(
                "Getting number of edges to prune took too long. Getting total number of edges instead"
            )
            n_edges_to_prune = self.arango.get_number_of_edges()
        except ReadTimeout:
            logger.warning(
                "HTTP Request to get number of edges to prune took too long. Getting total number of edges instead"
            )
            n_edges_to_prune = self.arango.get_number_of_edges()
        logger.info(f"Number of all edges to prune: {n_edges_to_prune}")
        # n_edges_to_prune = self.arango.get_number_of_edges()
        # logger.info(f"Number of edges to prune: {n_edges_to_prune}")

        while True:
            cursor_edges = self.arango.get_edges_to_prune(  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
                timestamp=self.timestamp_to_prune, batch_size=self.batch_size_query
            )  # Have to stay like this to avoid empty cursor
            logger.info(f"Fetched {cursor_edges.count()} edges to prune in this batch")  # pyright: ignore [reportUnknownMemberType,reportAttributeAccessIssue,reportOptionalMemberAccess]
            # if not len(list(cursor_edges)):
            #     break
            if not cursor_edges.count():  # pyright: ignore [reportUnknownMemberType,reportAttributeAccessIssue,reportOptionalMemberAccess]
                break
            if self.arango.chain_id is None:
                raise ValueError("chain_id is not set in ArangoDB client")
            edges_to_prune = [
                Edge(
                    chain_id=self.arango.chain_id,
                    from_address=doc["_from"].split("_")[-1],  # pyright: ignore [reportUnknownArgumentType,reportUnknownMemberType]
                    to_address=doc["_to"].split("_")[-1],  # pyright: ignore [reportUnknownArgumentType,reportUnknownMemberType]
                    transfer_logs=doc["tokenTransferLogs"],  # pyright: ignore [reportUnknownArgumentType]
                    oldest_transfer_at=doc.get("oldestTransferAt", None),  # pyright: ignore [reportUnknownArgumentType,reportUnknownMemberType]
                )
                for doc in cursor_edges.batch()  # pyright: ignore [reportUnknownMemberType,reportAttributeAccessIssue,reportOptionalMemberAccess,reportUnknownVariableType]
            ]

            _job = _GraphEdgesPruner(
                chain_id=self.chain_id,
                edges=edges_to_prune,
                batch_size=self.batch_size_thread,
                max_workers=self.max_workers,
                arango=self.arango,
                timestamp_to_prune=self.timestamp_to_prune,
            )
            _job.run()
            _count += self.batch_size_query
            del _job
            logger.info(f"Edges Pruning: {_count} /{n_edges_to_prune} edges. ")

        _ = gc.collect()
        logger.info(f"Finished pruning {_count} / {n_edges_to_prune} edges")

    def _update_vertices(self):
        """After pruning, update numberSent/numberReceived/tags for vertices"""
        _count = 0
        self.arango.set_filter(self.start_timestamp)
        _number_of_works = self.arango.get_number_of_vertices_to_update()
        logger.info(
            f"Number of vertices on chain {self.chain_id} to update: {_number_of_works}"
        )

        while True:
            keys_to_prune: list[str] = list(
                self.arango.get_vertex_keys_to_update(limit=self.batch_size_query)
            )
            if not keys_to_prune:
                break

            _job = _GraphVerticesUpdater(
                chain_id=self.chain_id,
                vertex_keys=keys_to_prune,
                batch_size=self.batch_size_thread,
                max_workers=self.max_workers,
                arango=self.arango,
            )
            _job.run()

            _count += self.batch_size_query
            logger.info(
                f"Updated {_count} / {_number_of_works} vertices prune. Overall Progress: {_count / _number_of_works * 100:.2f} %"
            )
            del _job

        _ = gc.collect()
        logger.info(f"Finished updating {_count} / {_number_of_works} vertices")

    @override
    def _end(self):
        logger.info("Start removing dangling vertices")
        self.arango.remove_all_dangling_vertices()

        logger.info(
            f"Finished execute from {human_readable_time(self.start_timestamp)} to {human_readable_time(self.next_synced_timestamp)}"
        )
        self.start_timestamp = self.next_synced_timestamp
        write_last_synced_file(self.last_synced_file, self.start_timestamp)
        time.sleep(3)


class _GraphVerticesPruner(BaseJob):
    def __init__(
        self,
        chain_id: str,
        vertex_keys: list[str],
        batch_size: int,
        max_workers: int,
        arango: _Arango,
        timestamp_to_prune: int,
    ):
        self.chain_id: str = chain_id
        if Chains.names.get(chain_id) is None:
            raise ValueError(f"Chain {chain_id} is not supported")
        else:
            self.chain_name: str = str(Chains.names.get(chain_id))
        self.arango: _Arango = arango
        self.timestamp_to_prune: int = timestamp_to_prune

        self.batch_size: int = batch_size
        self.max_workers: int = max_workers
        super().__init__(  # pyright: ignore [reportUnknownMemberType]
            work_iterable=vertex_keys, batch_size=batch_size, max_workers=max_workers
        )

    @override
    def _execute_batch(self, works: list[str]):
        self.arango.remove_vertices(vertex_keys=works)


class _GraphEdgesPruner(BaseJob):
    def __init__(
        self,
        chain_id: str,
        edges: list[Edge],
        batch_size: int,
        max_workers: int,
        arango: _Arango,
        timestamp_to_prune: int,
    ):
        self.chain_id: str = chain_id
        if Chains.names.get(chain_id) is None:
            raise ValueError(f"Chain {chain_id} is not supported")
        else:
            self.chain_name: str = str(Chains.names.get(chain_id))
        self.arango: _Arango = arango
        self.timestamp_to_prune: int = timestamp_to_prune

        self.batch_size: int = batch_size
        self.max_workers: int = max_workers
        super().__init__(  # pyright: ignore [reportUnknownMemberType]
            work_iterable=edges, batch_size=batch_size, max_workers=max_workers
        )

    @override
    def _execute_batch(self, works: list[Edge]):
        for edge in works:
            edge.prune_transfers(timestamp_to_prune=self.timestamp_to_prune)

        edges_to_update: list[TransferToGraphSchema] = list()
        edge_keys_to_delete: list[str] = list()
        for e in works:
            if e.oldest_transfer_at:
                edges_to_update.append(
                    {
                        "_key": e.key,
                        "tokenTransferLogs": e.get_transfer_logs(),
                        "oldestTransferAt": e.oldest_transfer_at,
                    }
                )
            else:
                edge_keys_to_delete.append(e.key)

        self.arango.update_edges(data=edges_to_update)
        self.arango.remove_edges(edge_keys=edge_keys_to_delete)


class _GraphVerticesUpdater(BaseJob):
    def __init__(
        self,
        chain_id: str,
        vertex_keys: list[str],
        batch_size: int,
        max_workers: int,
        arango: _Arango,
    ):
        self.chain_id: str = chain_id
        if Chains.names.get(chain_id) is None:
            raise ValueError(f"Chain {chain_id} is not supported")
        else:
            self.chain_name: str = str(Chains.names.get(chain_id))
        self.arango: _Arango = arango

        self.batch_size: int = batch_size
        self.max_workers: int = max_workers
        super().__init__(  # pyright: ignore [reportUnknownMemberType]
            work_iterable=vertex_keys, batch_size=batch_size, max_workers=max_workers
        )

    @override
    def _execute_batch(self, works: list[str]):
        updated_docs_cursor = self.arango.update_number_sent_received(works)  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
        non_human_addresses = [  # pyright: ignore [reportUnknownVariableType]
            doc["address"]
            for doc in updated_docs_cursor  # pyright: ignore [reportUnknownVariableType,reportGeneralTypeIssues,reportOptionalIterable]
            if doc["numberSent"] > BOT_THRESHOLD
        ]
        if non_human_addresses:  # assign tag (bots/contracts)
            self._assign_tag_for_address(non_human_addresses)  # pyright: ignore [reportUnknownArgumentType]

    def _assign_tag_for_address(self, addresses: list[str]):
        """assign tag (bots/contracts) on Arango for addresses"""
        provider_url = Networks.providers.get(self.chain_name)
        if provider_url is None:
            raise ValueError(f"Provider for chain {self.chain_name} is not supported")
        if_contracts = check_if_contracts(
            addresses=addresses, provider_url=provider_url
        )
        contract_keys: list[str] = list()
        bot_keys: list[str] = list()

        for addr, is_contract in if_contracts.items():
            if is_contract:
                contract_keys.append(f"{self.chain_id}_{addr}")
            else:
                bot_keys.append(f"{self.chain_id}_{addr}")
        self.arango.set_tag_for_wallet_vertex(keys=bot_keys, tag="bot")
        self.arango.set_contract_tag_for_vertex(keys=contract_keys)
        self.arango.set_number_sent_to_null(vertex_keys=contract_keys + bot_keys)

    # def _remove_dangling_vertices(self, vertex_keys: list[str]):
    #     _query = f"""FOR w IN {self.addresses_col_name}
    #         FILTER w._key in {vertex_keys}
    #             AND (w.numberSent == 0 AND w.numberReceived == 0)
    #         REMOVE {{_key: w._key}} IN {self.addresses_col_name}
    #         OPTIONS {{ ignoreErrors: true}}"""
    #     self._db.aql.execute(_query)

    # def prune_affected_vertices(self):
    #     self._decrease_number_received(self._vertex_ids_to_prune)
    #     self._remove_dangling_vertices(list(set(self._vertex_ids_to_prune)))

    # def prune_outbound_edges(self, vertex_ids: list[str]):
    #     # Prune Outbound edges, and return keys of outbound vertex ids
    #     _query_prune = f"""FOR id IN {vertex_ids}
    #         FOR v, e IN 1..1 OUTBOUND id GRAPH '{self.transfers_graph_name}'
    #         REMOVE e._key IN {self.transfers_col_name}
    #         OPTIONS {{ ignoreErrors: true }}
    #         RETURN OLD._to"""
    #     _to_vertex_ids = list(self._db.aql.execute(_query_prune))
    #
    #     # set numberSent of bots/contracts to null
    #     vertex_keys = [_id.split('/')[-1] for _id in vertex_ids]
    #     self.set_number_sent_received_to_null(vertex_keys)
    #
    #     # self._vertex_ids_to_prune.extend(_to_vertex_ids)

    # def has_vertices_to_prune(self) -> bool:
    #     """If there have been no edges removed, then there is no need to remove dangling vertices"""
    #     if self._vertex_ids_to_prune:
    #         return True
    #     return False
    #
    # def reset_vertex_ids(self):
    #     self._vertex_ids_to_prune.clear()

    # def _decrease_number_received(self, vertex_keys: list[str]):
    #     _query_decrease = f"""FOR w IN {self.addresses_col_name}
    #         FILTER w._key in {vertex_keys} AND w.numberReceived
    #         UPDATE w WITH  {{
    #             _key: w._key,
    #             numberReceived: w.numberReceived - 1
    #         }} IN {self.addresses_col_name}
    #         OPTIONS {{mergeObjects: true}}"""
    #     self._db.aql.execute(_query_decrease)


# if __name__ == "__main__":
#     job = GraphPruneJob(
#         chain_id="0x38",
#         batch_size_query=10000,
#         batch_size_thread=1000,
#         max_workers=1,
#         start_timestamp=1710892800,
#         last_synced_file=".data/0x38_graph_enrich.txt",
#         # end_timestamp: int = None,
#         interval=TimeConstants.A_DAY,
#     )
#     job.run()
