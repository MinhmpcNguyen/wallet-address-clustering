import logging
from collections import namedtuple
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from clickhouse_sqlalchemy import (  # pyright: ignore [reportMissingTypeStubs]
    make_session,  # pyright: ignore [reportUnknownVariableType]
)
from constants.cassandra_constants import CassandraConstants
from sqlalchemy import (  # pyright: ignore [reportMissingTypeStubs]
    bindparam,  # pyright: ignore [reportUnknownVariableType]
    create_engine,  # pyright: ignore [reportUnknownVariableType]
    text,  # pyright: ignore [reportUnknownVariableType]
)
from sqlalchemy.engine import Result  # pyright: ignore [reportMissingTypeStubs]

logger = logging.getLogger("ClickHouse")
logger.setLevel(logging.INFO)


class CHResultSet:
    """
    Lightweight iterable that mimics Cassandra's ResultSet surface:
    - Iterable over rows
    - .one() returns the first row or None
    Rows are namedtuples exposing attribute access and ._asdict().
    """

    def __init__(self, rows: list[tuple[Any]], colnames: Sequence[str]):
        Row = namedtuple("Row", colnames)  # pyright: ignore [reportUntypedNamedTuple]
        RowT: type[Row] = Row
        self._rows: list[Row] = [RowT(*r) for r in rows]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._rows)

    def one(self) -> Any | None:
        return self._rows[0] if self._rows else None


class ClickHouseCentic:
    """
    ClickHouse adapter that preserves CassandraCentic's public API and return semantics.
    - Methods return an iterable ResultSet-like object with namedtuple rows and ._asdict().
    - Column names match the original Cassandra SELECTs.
    - Uses *_local tables according to your ClickHouse schema.
    """

    def __init__(
        self,
        url: str = "clickhouse+http://all_etl_reader:etl_reader_block_chain341@152.42.240.160:8123/default",
        default_schema: str | None = None,
        connect_args: dict[str, str] | None = None,
    ):
        self.engine = create_engine(url, connect_args=connect_args or {})  # pyright: ignore [reportUnannotatedClassAttribute]
        self.session = make_session(self.engine)  # pyright: ignore [reportUnannotatedClassAttribute]
        # Default to your CassandraConstants.schema if not provided
        self.default_schema: str | None = default_schema or CassandraConstants.schema

        # Physical tables in ClickHouse (local tables contain the data)
        self.tb_blocks: str = "blocks_local"  # ORDER BY (number, hash)
        self.tb_transactions: str = (
            "transactions_local"  # ORDER BY (block_number, transaction_index)
        )
        self.tb_tx_receipts: str = (
            "transaction_receipts_local"  # ORDER BY (block_number, transaction_index)
        )
        self.tb_token_transfer: str = (
            "token_transfer_local"  # ORDER BY (block_number, log_index)
        )

    # ---------- Helpers ----------
    def _schema(self, chain: str | None) -> str:
        """
        Follow the original convention:
        schema = f"{chain}_blockchain_etl" if chain else CassandraConstants.schema
        """
        if chain == "bsc":
            return "blockchain_etl"
        else:
            return f"{chain}_blockchain_etl"

    def _tbl(self, schema: str, name: str) -> str:
        return f"{schema}.{name}"

    @staticmethod
    def _wrap(sa_result: Result) -> CHResultSet:
        """
        Wrap a SQLAlchemy result into a Cassandra-like ResultSet with namedtuple rows.
        """
        colnames: Sequence[str] = list(map(str, sa_result.keys()))  # pyright: ignore [reportUnknownArgumentType]
        rows = [tuple(r) for r in sa_result.fetchall()]  # pyright: ignore [reportUnknownArgumentType,reportUnknownVariableType]
        return CHResultSet(rows, colnames)  # pyright: ignore [reportUnknownArgumentType]

    # ---------- Public API (same names) ----------
    def get_event_transfers(
        self, from_block: int, to_block: int, chain: str | None = None
    ) -> CHResultSet:
        """
        Cassandra: SELECT * FROM <schema>.<transfer_event_table> WHERE bucket_id IN (...) AND block_number range.
        ClickHouse: token_transfer_local has no bucket_id; filter by block_number range.
        Return columns: use SELECT * to mirror Cassandra's 'SELECT *' behavior.
        """
        schema = self._schema(chain)
        # SELECT * to keep column names identical to the table definition
        stmt = text(f"""
            SELECT *
            FROM {self._tbl(schema, self.tb_token_transfer)}
            WHERE block_number >= :from_block
              AND block_number <= :to_block
        """)  # pyright: ignore [reportCallIssue]
        res: Result = self.session.execute(  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
            stmt, {"from_block": from_block, "to_block": to_block}
        )
        return self._wrap(res)  # pyright: ignore [reportUnknownArgumentType]

    def get_num_blocks(
        self, bucket_id: int, table: str, chain: str | None = None
    ) -> CHResultSet:
        """
        Cassandra: SELECT * FROM <schema>.<table> WHERE bucket_id IN (bucket_id)
        ClickHouse has no bucket_id. We preserve the method signature and emulate
        bucket windows using partition rules:
          - blocks_local: PARTITION BY intDiv(number, 100000)
          - most others: PARTITION BY intDiv(block_number, 1000)
        """
        schema = self._schema(chain)

        # Decide which key to use
        if table == self.tb_blocks:
            # Map bucket_id partition to number range
            from_block = int(bucket_id) * 100000
            to_block = (int(bucket_id) + 1) * 100000 - 1
            # Preserve original projection: the Cassandra code did SELECT *
            schemas = self._tbl(schema, self.tb_blocks)
            stmt = text(f"""
                SELECT *
                FROM {schemas}
                WHERE number BETWEEN :from_block AND :to_block
            """)  # pyright: ignore [reportCallIssue]
            params = {"from_block": from_block, "to_block": to_block}
        else:
            # For other tables, assume block_number partition of 1000
            from_block = int(bucket_id) * 1000
            to_block = (int(bucket_id) + 1) * 1000 - 1
            stmt = text(f"""
                SELECT *
                FROM {self._tbl(schema, table)}
                WHERE block_number BETWEEN :from_block AND :to_block
            """)  # pyright: ignore [reportCallIssue]
            params = {"from_block": from_block, "to_block": to_block}

        res = self.session.execute(stmt, params)  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
        return self._wrap(res)  # pyright: ignore [reportUnknownArgumentType]

    def get_blocks_in_range(
        self, from_block: int, to_block: int, chain: str | None = None
    ) -> CHResultSet:
        """
        Cassandra: SELECT number, timestamp FROM <schema>.<block_table> WHERE bucket_id IN (...) AND number range.
        ClickHouse: filter by number range on blocks_local.
        Column names preserved: number, timestamp.
        """
        schema = self._schema(chain)
        stmt = text(f"""
            SELECT number, timestamp
            FROM {self._tbl(schema, self.tb_blocks)}
            WHERE number >= :from_block
              AND number <= :to_block
        """)  # pyright: ignore [reportCallIssue]
        res = self.session.execute(  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
            stmt, {"from_block": from_block, "to_block": to_block}
        )
        return self._wrap(res)  # pyright: ignore [reportUnknownArgumentType]

    def get_block_number_to_timestamp(
        self, from_block: int, to_block: int, chain: str | None = None
    ) -> dict[int, int]:
        """
        Preserve the original mapping with row._asdict().
        """
        rows = self.get_blocks_in_range(
            from_block=from_block, to_block=to_block, chain=chain
        )
        return {row._asdict()["number"]: row._asdict()["timestamp"] for row in rows}

    def get_native_transfer_txs(
        self, from_block: int, to_block: int, chain: str | None = None
    ) -> CHResultSet:
        """
        Cassandra selected: from_address, to_address, value, block_timestamp, hash, receipt_contract_address
        with filters:
          - block_number range
          - input = '0x'
          - value > '0' (string compare in Cassandra; here cast to numeric for correctness)
          - receipt_status = 1
        In ClickHouse, receipt fields are in transaction_receipts_local, so we join:
          transactions_local AS t
          INNER JOIN transaction_receipts_local AS r
        We alias columns to match the exact Cassandra SELECT names.
        """
        schema = self._schema(chain)
        stmt = text(f"""
            SELECT
              t.from_address                     AS from_address,
              t.to_address                       AS to_address,
              t.value                            AS value,
              t.block_timestamp                  AS block_timestamp,
              t.hash                             AS hash,
              r.receipt_contract_address         AS receipt_contract_address
            FROM {self._tbl(schema, self.tb_transactions)} AS t
            INNER JOIN {self._tbl(schema, self.tb_tx_receipts)} AS r
              ON t.hash = r.hash
             AND t.transaction_index = r.transaction_index
             AND t.block_number = r.block_number
            WHERE t.block_number BETWEEN :from_block AND :to_block
              AND t.input = '0x'
              AND coalesce(toInt256OrNull(t.value), toInt256(0)) > 0
              AND r.receipt_status = 1
        """)  # pyright: ignore [reportCallIssue]
        res = self.session.execute(  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
            stmt, {"from_block": from_block, "to_block": to_block}
        )
        return self._wrap(res)  # pyright: ignore [reportUnknownArgumentType]

    def get_token_transfer_senders_by_receivers(
        self,
        to_addresses: Iterable[str],
        from_block: int,
        to_block: int,
        chain: str | None,
    ) -> CHResultSet:
        """
        Cassandra selected: from_address, to_address from transfer_event_table with:
          - bucket_id IN (...)
          - to_address IN (...)
          - block_number range
        ClickHouse: token_transfer_local; no bucket_id; same projections and filters (without bucket).
        Column names preserved: from_address, to_address.
        """
        schema = self._schema(chain)
        to_list = list(to_addresses) or ["__never__"]
        stmt = text(f"""
                SELECT
                  from_address,
                  to_address
                FROM {self._tbl(schema, self.tb_token_transfer)}
                WHERE to_address IN :to_addresses
                  AND block_number BETWEEN :from_block AND :to_block
                """)  # pyright: ignore [reportCallIssue]
        stmt = stmt.bindparams(bindparam("to_addresses", expanding=True))  # pyright: ignore [reportCallIssue,reportUnknownMemberType,reportAttributeAccessIssue,reportUnknownVariableType]
        res = self.session.execute(  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
            stmt,
            {"to_addresses": to_list, "from_block": from_block, "to_block": to_block},
        )
        return self._wrap(res)  # pyright: ignore [reportUnknownArgumentType]

    def get_transaction_senders_by_receivers(
        self,
        to_addresses: Iterable[str],
        from_block: int,
        to_block: int,
        chain: str | None,
    ) -> CHResultSet:
        """
        Cassandra selected: from_address, to_address from transactions_table with:
          - to_address IN (...)
          - block_number range
        ClickHouse: transactions_local; same projections and filters.
        Column names preserved: from_address, to_address.
        """
        schema = self._schema(chain)
        to_list = list(to_addresses) or ["__never__"]
        stmt = text(f"""
                SELECT
                  from_address,
                  to_address
                FROM {self._tbl(schema, self.tb_transactions)}
                WHERE to_address IN :to_addresses
                  AND block_number BETWEEN :from_block AND :to_block
                """)  # pyright: ignore [reportCallIssue]
        stmt = stmt.bindparams(bindparam("to_addresses", expanding=True))  # pyright: ignore [reportCallIssue,reportUnknownMemberType,reportAttributeAccessIssue,reportUnknownVariableType]
        res = self.session.execute(  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
            stmt,
            {"to_addresses": to_list, "from_block": from_block, "to_block": to_block},
        )
        return self._wrap(res)  # pyright: ignore [reportUnknownArgumentType]
