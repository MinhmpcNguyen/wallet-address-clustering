from collections.abc import Iterator, Sequence
from typing import Any

from arango.client import ArangoClient
from arango.collection import StandardCollection
from arango.cursor import Cursor
from arango.database import StandardDatabase
from arango.http import DefaultHTTPClient
from arango.job import AsyncJob, BatchJob
from arango.result import Result
from arango.typings import Json
from config import ArangoDBConfig
from constants.network_constants import Chains
from utils.parser import get_connection_elements
from utils.retry_handler import retry_handler


class ArangoBase:
    def __init__(
        self,
        *,
        connection_url: str = ArangoDBConfig.CONNECTION_URL,
        database: str = ArangoDBConfig.DATABASE,
    ):
        if not connection_url:
            connection_url = ArangoDBConfig.CONNECTION_URL
        username, password, clean_host = get_connection_elements(connection_url)
        http_client = DefaultHTTPClient(request_timeout=None)
        self.client: ArangoClient = ArangoClient(
            hosts=clean_host, http_client=http_client
        )
        self.db: StandardDatabase = self.client.db(
            database, username=username, password=password
        )

    def ensure_collection(
        self, name: str, *, edge: bool = False, shard_count: int = 20
    ) -> StandardCollection:
        if not self.db.has_collection(name):
            _ = self.db.create_collection(name, edge=edge, shard_count=shard_count)
        return self.db.collection(name)

    def ensure_graph(self, name: str, edge_definitions: list[dict[str, Any]]):
        if not self.db.has_graph(name):
            _ = self.db.create_graph(name, edge_definitions=edge_definitions)
        return self.db.graph(name)

    @retry_handler(retries_number=3, sleep_time=5)
    def aql(
        self,
        query: str,
        *,
        bind_vars: dict[str, Any] | None = None,
        batch_size: int | None = None,
        ttl: int | None = None,
        count: bool | None = None,
        stream: bool | None = None,
        max_runtime: int | None = None,
    ) -> Any:
        """
        Run raw AQL query.
        This may return Cursor, AsyncJob, BatchJob, list, or None.
        """
        kwargs: dict[str, Any] = {"query": query}
        if bind_vars is not None:
            kwargs["bind_vars"] = bind_vars
        if batch_size is not None:
            kwargs["batch_size"] = batch_size
        if ttl is not None:
            kwargs["ttl"] = ttl
        if count is not None:
            kwargs["count"] = count
        if stream is not None:
            kwargs["stream"] = stream
        if max_runtime is not None:
            kwargs["max_runtime"] = max_runtime
        return self.db.aql.execute(**kwargs)

    # def _normalize_result_iter(self, obj: Any) -> Iterator[Mapping[str | int, Any]]:
    #     if obj is None:
    #         return iter(())
    #     if isinstance(obj, Cursor):
    #         return cast(Iterator[Mapping[str | int, Any]], iter(obj))
    #     if isinstance(obj, AsyncJob):
    #         return self._normalize_result_iter(obj.result())
    #     if isinstance(obj, BatchJob):
    #         results = cast(list[Any], obj.result())
    #         return chain.from_iterable(self._normalize_result_iter(r) for r in results)
    #     if isinstance(obj, (list, tuple)):

    #         def _it() -> Iterator[Mapping[str | int, Any]]:
    #             for it in obj:
    #                 yield cast(Mapping[str | int, Any], it)

    #         return _it()
    #     if hasattr(obj, "__iter__"):
    #         return cast(Iterator[Mapping[str | int, Any]], obj)
    #     raise TypeError(f"Unsupported query result type: {type(obj)!r}")
    def _normalize_result_iter(self, obj: Any) -> Iterator[Any]:
        if obj is None:
            return iter(())
        if isinstance(obj, Cursor):
            return iter(obj)  # items có thể là int/str/dict
        if isinstance(obj, AsyncJob):
            return self._normalize_result_iter(obj.result())
        if isinstance(obj, BatchJob):
            results = obj.result()  # pyright: ignore [reportUnknownVariableType]

            def _it() -> Iterator[Any]:
                for r in results:  # pyright: ignore [reportUnknownVariableType]
                    yield from self._normalize_result_iter(r)

            return _it()
        if isinstance(obj, (list, tuple)):
            return iter(obj)  # pyright: ignore [reportUnknownVariableType,reportUnknownArgumentType]
        if hasattr(obj, "__iter__"):
            return obj
        raise TypeError(f"Unsupported query result type: {type(obj)!r}")

    def query(
        self,
        query: str,
        *,
        bind_vars: dict[str, Any] | None = None,
        batch_size: int = 1000,
        ttl: int = 100,
        count: bool | None = None,
        stream: bool = True,
    ) -> Iterator[Any]:
        """
        Safe query method.
        Always returns a Cursor so it can be iterated without type errors.
        """
        raw = self.aql(
            query,
            bind_vars=bind_vars,
            batch_size=batch_size,
            ttl=ttl,
            count=count,
            stream=stream,
        )
        return self._normalize_result_iter(raw)


class AddressGraphClient(ArangoBase):
    def __init__(
        self,
        prefix: str,
        *,
        connection_url: str = ArangoDBConfig.CONNECTION_URL,
        database: str = ArangoDBConfig.DATABASE,
    ):
        # keep the same behavior for host parsing and db auth
        super().__init__(connection_url=connection_url, database=database)
        self.prefix: str = prefix
        self.chain_id: str | None = Chains.mapping.get(prefix)
        # Names
        self.addresses_col_name: str = f"{prefix}_addresses"
        self.transfers_col_name: str = f"{prefix}_transfers"
        self.transfers_graph_name: str = f"{prefix}_transfers_graph"
        self.addresses_col: str = self.col_name("addresses")
        self.transfers_col: str = self.col_name("transfers")
        self.graph_name: str = self.graph_name_fn("transfers_graph")

        # Ensure resources
        self._addresses_handle: StandardCollection = self.ensure_collection(
            self.addresses_col, edge=False
        )
        self._addresses_col: StandardCollection = self._get_collections(
            self.addresses_col_name
        )
        self._transfers_col: StandardCollection = self._get_collections(
            self.transfers_col_name, edge=True
        )
        self._transfers_handle: StandardCollection = self.ensure_collection(
            self.transfers_col, edge=True
        )
        _ = self.ensure_graph(
            self.graph_name,
            edge_definitions=[
                {
                    "edge_collection": self.transfers_col,
                    "from_vertex_collections": [self.addresses_col],
                    "to_vertex_collections": [self.addresses_col],
                }
            ],
        )

    def _get_collections(
        self,
        collection_name: str,
        database: StandardDatabase | None = None,
        edge: bool = False,
    ) -> StandardCollection:
        if not database:
            database = self.db
        if not database.has_collection(collection_name):
            _ = database.create_collection(collection_name, shard_count=20, edge=edge)
        return database.collection(collection_name)

    def _get_graph(
        self,
        graph_name: str,
        edge_definitions: Sequence[Json],
        database: StandardDatabase | None = None,
    ):
        if not database:
            database = self.db
        if not database.has_graph(graph_name):
            _ = database.create_graph(graph_name, edge_definitions=edge_definitions)
        return database.graph(graph_name)

    # -------- naming helpers --------
    def col_name(self, short: str) -> str:
        return f"{self.prefix}_{short}"

    def graph_name_fn(self, short: str) -> str:
        return f"{self.prefix}_{short}"

    # -------- counts (from code one) --------
    def get_number_of_addresses(self) -> Result[int]:
        return self._addresses_handle.count()

    def get_number_of_edges(self) -> Result[int]:
        return self._transfers_handle.count()

    # -------- generic address queries --------
    def get_addresses_by_in(
        self,
        addresses: list[str],
        *,
        fields: list[str] | None = None,
        batch_size: int = 10000,
    ) -> Result[Cursor]:
        if fields is None:
            fields = ["_key", "address", "numberSent", "numberReceived"]

        projection = ", ".join(
            [f"'{f}': v.{f}" if f != "_key" else "'_key': v._key" for f in fields]
        )
        q = f"""
            FOR v IN {self.addresses_col}
            FILTER v.address IN @addrs
            RETURN {{ {projection} }}
        """
        return self.aql(
            q, bind_vars={"addrs": list(addresses)}, batch_size=batch_size, count=True
        )

    def get_wallets_by_flag(
        self,
        flag: str,
        *,
        fields: list[str] | None = None,
        batch_size: int = 10000,
        ttl: int = 600,
    ) -> Result[Cursor]:
        if fields is None:
            fields = ["_key", "address", "numberSent", "numberReceived"]
        projection = ", ".join(
            [f"'{f}': v.{f}" if f != "_key" else "'_key': v._key" for f in fields]
        )
        q = f"""
            FOR v IN {self.addresses_col}
            FILTER v.wallet.{flag}
            RETURN {{ {projection} }}
        """
        return self.aql(q, batch_size=batch_size, ttl=ttl, count=True)

    # Thin wrappers to preserve your external API
    def get_vertices_by_addresses(self, addresses: list[str], batch_size: int = 10000):
        return self.get_addresses_by_in(addresses, batch_size=batch_size)

    def get_deposit_wallets(self, batch_size: int = 10000):
        return self.get_wallets_by_flag("depositWallet", batch_size=batch_size)

    def get_tagged_wallets(self, tag: str, batch_size: int = 10000):
        q = f"""
            FOR v IN {self.addresses_col}
            FILTER v.wallet.{tag}
            RETURN v
        """
        return self.aql(q, batch_size=batch_size, count=True)

    # -------- merged features from code one --------
    def get_all_recent_addresses_not_hot_wallets(
        self, last_transfer_timestamp: int, ttl: int = 7200, batch_size: int = 1000
    ) -> Result[Cursor]:
        """
        Return addresses where wallet.hotWallet != true and lastTransferAt > given ts.
        """
        q = f"""
            FOR doc IN {self.addresses_col}
                FILTER !doc.wallet.hotWallet
                AND doc.lastTransferAt > @ts
            RETURN doc.address
        """
        return self.aql(
            q,
            bind_vars={"ts": last_transfer_timestamp},
            ttl=ttl,
            batch_size=batch_size,
            stream=True,
        )

    def delete_name(
        self, chain_id: str, name: str, last_updated_at: int
    ) -> Result[Cursor]:
        """
        Remove a name from w.names for all docs with given chainId.
        """
        q = f"""
            FOR w IN {self.addresses_col}
                FILTER w.chainId == @cid
                AND @nm IN w.names
                UPDATE w WITH {{
                    names: REMOVE_VALUE(w.names, @nm),
                    lastUpdatedAt: @ts
                }} IN {self.addresses_col}
        """
        return self.aql(
            q, bind_vars={"cid": chain_id, "nm": name, "ts": last_updated_at}
        )

    def update_name(
        self, chain_id: str, address: str, name: str, last_updated_at: int
    ) -> Result[Cursor]:
        """
        Safely push a name into w.names (unique) for _key == "{chain_id}_{address}".
        Uses bind variables to avoid quoting issues.
        """
        key = f"{chain_id}_{address}"
        q = f"""
            FOR w IN {self.addresses_col}
                FILTER w._key == @key
                UPDATE w WITH {{
                    names: PUSH(w.names, @nm, true),
                    lastUpdatedAt: @ts
                }} IN {self.addresses_col}
        """
        return self.aql(q, bind_vars={"key": key, "nm": name, "ts": last_updated_at})

    def check_has_address(self, chain_id: str, address: str) -> Result[bool]:
        """
        Check if a vertex with _key == "{chain_id}_{address}" exists.
        """
        key = f"{chain_id}_{address}"
        # use collection.has for O(1) existence check
        return self._addresses_handle.has(key)
