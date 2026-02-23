import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from multithread_processing.base_job import (  # pyright: ignore [reportMissingTypeStubs]
    BaseJob,
)
from schemas.subgraph_schema import SubgraphDoc

sys.path.append(os.path.dirname(sys.path[0]))
from constants.network_constants import Chains
from databases.arangodb_klg import AddressGraphClient
from databases.mongodb import MongoDB
from typing_extensions import override
from utils.logger_utils import get_file_handler, get_logger

LIMIT_NUMBER_OF_EDGES = 10000
logger = get_logger("Subgraphs Loader (to Mongo) Job")
logger.addHandler(get_file_handler("subgraph_exporter_job.log"))


class SubgraphExporterJob(BaseJob):
    """Export subgraphs from Arango to Mongo"""

    def __init__(
        self,
        importer: AddressGraphClient,
        exporter: MongoDB,
        chain_id: str,
        addresses: list[str],
        radius: int = 2,
        batch_size: int = 100,
        max_workers: int = 4,
    ):
        """
        Args:
            self: Represent the instance of the class
            chain_id: Specify the chain that we want to extract data from
            batch_size: Set the number of work for each worker to process parallely
            max_workers: Limit the number of workers that can be used to process the work_iterable
        """
        self.exporter: MongoDB = exporter
        self.importer: AddressGraphClient = importer

        self.chain_id: str = chain_id
        if Chains.names.get(chain_id) is None:
            raise ValueError(f"Chain {chain_id} is not supported")
        else:
            self.chain_name: str = str(Chains.names.get(chain_id))
        self.radius: int = radius

        self.work_iterable: list[str] = addresses
        # self.work_iterable = ['0x000000000354712c33007d093547eb723b52cf4c']

        self._work_iterable_tracking: dict[str, int] = {
            addr: i for i, addr in enumerate(self.work_iterable)
        }
        self._progress_tracking: int = 0
        self.batch_size: int = batch_size
        self.max_workers: int = max_workers
        super().__init__(  # pyright: ignore [reportUnknownMemberType]
            work_iterable=self.work_iterable,
            batch_size=batch_size,
            max_workers=self.max_workers,
        )

    @override
    def _start(self):
        logger.info("Start exporting subgraphs from Arango to Mongo")

    # --- New: isolate the per-address logic so it can run in parallel ---
    def _process_single_address(self, address: str) -> SubgraphDoc | None:
        """
        Process a single address and return a SubgraphDoc or None.
        This function encapsulates the original logic for one address.
        """
        try:
            # Calculate numberSent/Received for neighbors (a.k.a depth = 1)
            _neighbors_cursor = self.importer.query(
                query=f"""
                FOR v, e IN 1..1 ANY '{self.importer.addresses_col_name}/{self.chain_id}_{address}'
                GRAPH {self.importer.transfers_graph_name}
                FILTER NOT (v.wallet.hotWallet OR v.wallet.depositWallet OR v.wallet.bot)
                RETURN {{neighbor: v._key, edge: e._key}}
                """,
                batch_size=self.batch_size,
            )
            neighbor_keys: list[str] = []
            neighbor_edge_keys: list[str] = []
            for doc in _neighbors_cursor:
                neighbor_keys.append(doc["neighbor"])
                neighbor_edge_keys.append(doc["edge"])
            neighbor_keys = list(set(neighbor_keys))
            # neighbor_edge_keys = list(set(neighbor_edge_keys))
            _time_now = int(time.time())
            _neighbors_data_query = f"""
                FOR k in {neighbor_keys}
                    LET id = CONCAT('{self.importer.addresses_col_name}/', k)
                    LET n_to = (FOR v IN 1..1 OUTBOUND id
                        GRAPH {self.importer.transfers_graph_name}
                        COLLECT WITH COUNT INTO n
                        RETURN n)
                    LET n_from = (FOR v IN 1..1 INBOUND id
                        GRAPH {self.importer.transfers_graph_name}
                        COLLECT WITH COUNT INTO n
                        RETURN n)
                    UPDATE {{_key: k, numberSent: n_to[0], numberReceived: n_from[0], lastUpdatedAt:{_time_now} }} IN {self.importer.addresses_col_name}
                    OPTIONS {{ ignoreErrors: true }}
                    LET updated = NEW
                    RETURN {{_key: k, numberSent: updated.numberSent, numberReceived: updated.numberReceived}}
            """
            _neighbors_data = list(self.importer.query(query=_neighbors_data_query))
            # logger.info(f"done update data for neighbors of {address}: {int(time.time()) - _time_now}s")

            # Get neighbors of neighbors
            _subgraph_2_edge_keys: list[str] = []
            neighbor_keys_to_traverse: list[str] = []
            # r1
            _query = f"""
                FOR v, e IN 1..1 ANY '{self.importer.addresses_col_name}/{self.chain_id}_{address}'
                GRAPH {self.importer.transfers_graph_name}
                FILTER NOT v.wallet.hotWallet 
                AND NOT v.wallet.bot
                RETURN {{'vertex': v, 'edge': e}}
            """
            _all_neighbors_cursor = self.importer.query(_query)
            for _neighbor in _all_neighbors_cursor:
                _subgraph_2_edge_keys.append(_neighbor["edge"]["_key"])
                try:
                    if (
                        _neighbor["vertex"]["numberSent"] < 100
                        and _neighbor["vertex"]["numberReceived"] < 100
                    ):
                        neighbor_keys_to_traverse.append(_neighbor["vertex"]["_key"])
                except Exception:
                    # Keep exactly the same fallback behavior as original code
                    print(_neighbor["vertex"])

            # r2
            for k in neighbor_keys_to_traverse:
                try:
                    _edge_keys_query = f"""
                        FOR v, e in 1..1 ANY '{self.importer.addresses_col_name}/{k}'
                        GRAPH {self.importer.transfers_graph_name}
                        FILTER NOT v.wallet.hotWallet
                        RETURN e
                    """
                    _edge_keys_cursor = self.importer.query(_edge_keys_query)
                    _edge_keys = [e["_key"] for e in _edge_keys_cursor]
                    _subgraph_2_edge_keys.extend(_edge_keys)
                except Exception as ex:
                    logger.exception(f"Cant't get neighbors of neighbor {k}: {ex}")

            _subgraph_2_edge_keys.extend(neighbor_edge_keys)
            _subgraph_2_edge_keys_set: set[str] = set(_subgraph_2_edge_keys)
            _subgraph_2_edges: list[dict[str, str]] = [
                {"from": str(k).split("_")[1], "to": str(k).split("_")[2]}
                for k in _subgraph_2_edge_keys_set
            ]

            if _subgraph_2_edges:
                _subgraph: SubgraphDoc = {
                    "_id": f"{self.chain_id}_{address}",
                    "chainId": self.chain_id,
                    "address": address,
                    "edges": _subgraph_2_edges,
                }
                return _subgraph

            return None

        except Exception as ex:
            # Fail-fast is not desired here; we log and continue to preserve original behavior resilience.
            logger.exception(f"Failed to build subgraph for {address}: {ex}")
            return None

    @override
    def _execute_batch(self, works: list[str]) -> None:
        # Run per-address work in parallel, then aggregate and write once.
        subgraphs_list: list[SubgraphDoc] = []

        # Important: We reuse self.max_workers to remain consistent with the job's config.
        # ThreadPoolExecutor is chosen because MongoClient and most HTTP drivers are thread-safe
        # and Arango's Python client issues independent queries per call.
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {
                executor.submit(self._process_single_address, address): address
                for address in works
            }

            for future in as_completed(future_map):
                result = future.result()
                if result is not None:
                    subgraphs_list.append(result)

        # Persist exactly once per batch (same logic as original)
        self.exporter.update_subgraphs(
            data=subgraphs_list,
            chain_name=self.chain_name,
            radius=self.radius,
            update_time=int(time.time()),
        )

        # Keep progress accounting identical to original behavior
        self._progress_tracking += self.batch_size
        logger.info(
            f"Exported subgraphs of addresses {self._work_iterable_tracking[works[0]]} "
            + f"to {self._work_iterable_tracking[works[-1]]} / {len(self.work_iterable)}. "
            + f"Progress: {self._progress_tracking / len(self.work_iterable) * 100:.2f} %"
        )

    @override
    def _end(self):
        self.batch_executor.shutdown()
