from __future__ import annotations

import time
from itertools import permutations
from logging import Logger
from typing import Any

import pandas as pd  # pyright: ignore [reportMissingTypeStubs]
from arango.cursor import Cursor
from arango.result import Result
from multithread_processing.base_job import (  # pyright: ignore [reportMissingTypeStubs]
    BaseJob,
)
from pandas import DataFrame  # pyright: ignore [reportMissingTypeStubs]
from pandas.core.series import Series  # pyright: ignore [reportMissingTypeStubs]
from pymongo import UpdateOne
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.database import Database
from typing_extensions import override

from constants.network_constants import Chains
from databases.arangodb_klg import AddressGraphClient
from databases.mongodb import MongoDB

# ---- structured errors
from hooks.errors import (
    AppError,
    ComputationError,
    ConfigError,
    DatabaseConnectionError,
    DatabaseError,
    DatabaseTimeoutError,
    DatabaseWriteError,
    DataParsingError,
    PerformanceError,
    PermissionError,
    QueryError,
    SchemaMismatchError,
)
from schemas.deposit_schema import DepositData, DepositReusePairDoc
from utils.logger_utils import get_logger

ARANGO_BATCH_SIZE = 50000
logger: Logger = get_logger("DepositReusePairJob")


class DepositReusePairJob:
    def __init__(
        self,
        chain: str,
        mongo_db: MongoDB,  # explicit MongoDB wrapper
        refresh_number_sent_received: bool = True,
        pairs_collection_name: str | None = None,  # custom output collection
        max_workers: int = 2,
        batch_size: int = 1000,
    ):
        self.logger: Logger = logger

        # chain -> chain_id
        try:
            self.chain_name: str = chain
            self.chain_id: str | None = Chains.mapping.get(chain, None)
            if not self.chain_id:
                raise ConfigError(
                    f"Invalid chain name: {chain}",
                    context={"chain": chain},
                    http_status=400,
                    error_code="INVALID_CHAIN",
                )
        except AppError:
            raise
        except Exception as e:
            raise ConfigError(
                "Failed to resolve chain_id from chain mapping",
                context={"chain": chain},
                cause=e,
            )

        self.refresh_number_sent_received: bool = refresh_number_sent_received
        self.max_workers: int = max_workers
        self.batch_size: int = batch_size

        # Arango client
        self.arango: AddressGraphClient
        try:
            self.arango = AddressGraphClient(prefix=self.chain_name)
        except PermissionError as e:  # if your driver raises custom
            raise PermissionError(
                "Permission denied creating ArangoDB client",
                context={"chain": self.chain_name},
                cause=e,
            )
        except Exception as e:
            # Most driver init failures map to infra/connection/auth
            raise DatabaseConnectionError(
                "Failed to initialize ArangoDB client",
                context={"chain": self.chain_name},
                cause=e,
            )

        # Mongo clients/collections (output)
        try:
            self.mongo_db: MongoDB = mongo_db
            self.db: Database[Any] = mongo_db.db
            self.pairs_col: Collection[DepositReusePairDoc] = self.db[
                pairs_collection_name or f"deposit_reuse_pairs_{self.chain_name}"
            ]
        except Exception as e:
            raise DatabaseConnectionError(
                "Failed to initialize MongoDB collections",
                context={"pairs_collection_name": pairs_collection_name},
                cause=e,
            )

        # Suggested indexes (best-effort)
        try:
            _ = self.pairs_col.create_index(
                [("X_address", 1), ("SubX_address", 1), ("chainId", 1)],
                name="pair_chain_unique",
                unique=True,
            )
            _ = self.pairs_col.create_index(
                [("deposit_address", 1)], name="deposit_idx"
            )
            _ = self.pairs_col.create_index([("updatedAt", 1)], name="updated_idx")
            _ = self.logger.debug("Indexes ensured on %s", self.pairs_col.name)
        except Exception as e:
            # Non-fatal, but record as DatabaseError to ease debugging
            self.logger.debug(
                "Index creation skipped/failed on %s: %s",
                self.pairs_col.name,
                e,
                exc_info=True,
            )

        # Buffers
        self.deposit_documents: list[DepositData] = []
        self.deposit_user_df: DataFrame = DataFrame()
        self.pairs_df: DataFrame = DataFrame()

    # -------------------- public API --------------------

    def run(self):
        t0 = time.time()
        self.logger.info(
            "Starting DepositReusePairJob (chain=%s, refresh_num_sr=%s, max_workers=%d, batch_size=%d)",
            self.chain_name,
            self.refresh_number_sent_received,
            self.max_workers,
            self.batch_size,
        )
        try:
            self._get_deposit_wallets()
            self._get_users_df()
            self._generate_pairs()  # writes directly to Mongo
        except AppError:
            # already mapped; just bubble up after logging
            self.logger.error("DepositReusePairJob failed", exc_info=True)
            raise
        except Exception as e:
            self.logger.error("DepositReusePairJob failed", exc_info=True)
            raise ComputationError(
                "Unhandled error running DepositReusePairJob",
                context={
                    "chain": self.chain_name,
                    "refresh_number_sent_received": self.refresh_number_sent_received,
                },
                cause=e,
            )
        finally:
            self.logger.info("Finished in %.2fs", time.time() - t0)

    # -------------------- internal steps --------------------

    def _get_deposit_wallets(self):
        self.logger.info("Fetching deposit accounts from ArangoDB…")

        # init & count
        try:
            deposits_cursor = self.arango.get_deposit_wallets(
                batch_size=ARANGO_BATCH_SIZE
            )
            try:
                number_deposits: int | Any | None = deposits_cursor.count()  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType,reportAttributeAccessIssue,reportOptionalMemberAccess]
            except Exception:
                # some cursor implementations compute count lazily / may fail
                self.logger.debug(
                    "Cursor.count() failed; falling back to unknown", exc_info=True
                )
                number_deposits = 0
            self.logger.info(
                "Total deposit accounts reported by cursor: %d",
                number_deposits,  # pyright: ignore [reportUnknownArgumentType]
            )
        except PermissionError as e:
            raise PermissionError(
                "Permission denied while creating deposit cursor",
                context={"batch_size": ARANGO_BATCH_SIZE},
                cause=e,
            )
        except TimeoutError as e:
            raise DatabaseTimeoutError(
                "Timeout initializing deposit cursor from ArangoDB",
                context={"batch_size": ARANGO_BATCH_SIZE},
                cause=e,
            )
        except Exception as e:
            raise DatabaseConnectionError(
                "Error while initializing deposit cursor from ArangoDB",
                context={"batch_size": ARANGO_BATCH_SIZE},
                cause=e,
            )

        _count = 0
        processed = 0
        while True:
            try:
                self.logger.info("Processing deposit batch #%d…", _count + 1)
                try:
                    batch_payload = deposits_cursor.batch()  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType,reportAttributeAccessIssue,reportOptionalMemberAccess]
                except Exception as e:
                    raise DatabaseError(
                        "Failed to fetch current batch from Arango cursor",
                        context={"batch_index": _count},
                        cause=e,
                    )

                try:
                    if not self.refresh_number_sent_received:
                        deposits_data: list[DepositData] = list(batch_payload)  # pyright: ignore [reportArgumentType]
                    else:
                        refresh_job = UpdateNumberSentReceivedJob(
                            addresses=[doc["address"] for doc in batch_payload],  # pyright: ignore [reportOptionalIterable,reportUnknownVariableType]
                            chain_id=self.chain_id,
                            chain_name=self.chain_name,
                            arango=self.arango,
                            batch_size=1000,
                            max_workers=4,
                        )
                        deposits_data = refresh_job.run()
                except AppError:
                    raise  # keep mapped error
                except Exception as e:
                    raise QueryError(
                        "Failed to refresh numberSent/numberReceived from Arango",
                        context={"batch_index": _count},
                        cause=e,
                    )

                try:
                    _filtered_deposits_data = self._filter_deposit_wallet(
                        deposit_wallets=deposits_data
                    )
                except AppError:
                    raise
                except Exception as e:
                    raise DataParsingError(
                        "Failed to filter deposit wallets",
                        context={"batch_index": _count, "items": len(deposits_data)},
                        cause=e,
                    )

                self.deposit_documents.extend(_filtered_deposits_data)

                processed += len(batch_payload)  # pyright: ignore [reportArgumentType]
                self.logger.info(
                    "Filtered %d (batch) / Accumulated %d deposits | PROGRESS: %.2f%%",
                    len(_filtered_deposits_data),
                    len(self.deposit_documents),
                    (processed / max(1, number_deposits)) * 100.0,  # pyright: ignore [reportArgumentType]
                )

                # Clear and advance
                try:
                    _ = batch_payload.clear()  # pyright: ignore [reportUnknownMemberType,reportOptionalMemberAccess,reportUnknownVariableType]
                except Exception:
                    # harmless if underlying cursor doesn't support list semantics
                    pass
                _count += 1

                # advance cursor
                try:
                    if (
                        hasattr(deposits_cursor, "has_more")
                        and deposits_cursor.has_more()  # pyright: ignore [reportUnknownMemberType,reportOptionalMemberAccess,reportAttributeAccessIssue]
                    ):
                        try:
                            _ = deposits_cursor.fetch()  # pyright: ignore [reportUnknownMemberType,reportOptionalMemberAccess,reportAttributeAccessIssue,reportUnknownVariableType]
                        except TimeoutError as e:
                            raise DatabaseTimeoutError(
                                "Timeout fetching next batch from ArangoDB",
                                context={"next_batch_index": _count + 1},
                                cause=e,
                            )
                        except Exception as e:
                            raise DatabaseError(
                                "Failed fetching next batch from ArangoDB",
                                context={"next_batch_index": _count + 1},
                                cause=e,
                            )
                    else:
                        break
                except AppError:
                    raise

            except AppError:
                self.logger.error(
                    "Error while processing deposit batch #%d",
                    _count + 1,
                    exc_info=True,
                )
                raise
            except Exception as e:
                self.logger.error(
                    "Error while processing deposit batch #%d",
                    _count + 1,
                    exc_info=True,
                )
                raise ComputationError(
                    "Unhandled error while processing deposit batch",
                    context={"batch_index": _count},
                    cause=e,
                )

        self.logger.info(
            "Collected %d qualifying deposit accounts", len(self.deposit_documents)
        )

    @staticmethod
    def _filter_deposit_wallet(
        deposit_wallets: list[DepositData],
        hot_wallets_limit: int = 3,
        user_wallets_limit: int = 20,
    ) -> list[DepositData]:
        # validate basic schema to give nicer errors
        for i, d in enumerate(deposit_wallets):
            if "numberSent" not in d or "numberReceived" not in d:
                raise SchemaMismatchError(
                    "Deposit wallet record missing required fields",
                    context={
                        "index": i,
                        "keys": list(d.keys()),
                    },
                )
        return [
            d
            for d in deposit_wallets
            if (
                d["numberSent"] < hot_wallets_limit
                and d["numberReceived"] < user_wallets_limit
            )
        ]

    def _get_users_df(self):
        try:
            deposit_addresses = [doc["address"] for doc in self.deposit_documents]
        except Exception as e:
            raise DataParsingError(
                "Malformed deposit_documents buffer",
                context={"items": len(self.deposit_documents)},
                cause=e,
            )

        self.logger.info(
            "Fetching users for %d deposit accounts…", len(deposit_addresses)
        )
        try:
            job = _GetUsersFromDepositsJob(
                arango=self.arango,
                chain_name=self.chain_name,
                chain_id=self.chain_id,
                deposit_addresses=deposit_addresses,
                batch_size=self.batch_size,
                max_workers=self.max_workers,
            )
            _users_list: list[dict[str, str]] = job.run()
        except AppError:
            raise
        except Exception as e:
            raise QueryError(
                "Failed to fetch users for deposits from Arango",
                context={"deposit_count": len(deposit_addresses)},
                cause=e,
            )

        self.logger.info("Raw user edges returned: %d", len(_users_list))

        try:
            self.deposit_user_df = pd.DataFrame.from_records(  # pyright: ignore [reportUnknownMemberType]
                [
                    {
                        "deposit_address": datum["deposit_address"].split("_")[-1],
                        "user_address": datum["user_address"].split("_")[-1],
                    }
                    for datum in _users_list
                ]
            )
        except Exception as e:
            raise DataParsingError(
                "Failed to construct deposit_user_df DataFrame",
                context={"records": len(_users_list)},
                cause=e,
            )

        self._filter_users_with_more_than_one_deposit()
        self._filter_bot_users()

        try:
            self.logger.info(
                "Unique deposits after filtering: %d",
                self.deposit_user_df["deposit_address"].nunique(),  # pyright: ignore [reportUnknownMemberType,reportUnknownArgumentType]
            )
            self.logger.info(
                "Unique users after filtering: %d",
                self.deposit_user_df["user_address"].nunique(),  # pyright: ignore [reportUnknownMemberType,reportUnknownArgumentType]
            )
            self.logger.debug(
                "Deposit-User describe()\n%s",
                self.deposit_user_df.describe(include="all"),  # pyright: ignore [reportUnknownMemberType]
            )
        except Exception:
            # Do not fail the pipeline for stats/describe issues
            self.logger.debug("Stats/describe failed on deposit_user_df", exc_info=True)

    def _filter_users_with_more_than_one_deposit(self):
        self.logger.info("Filtering users that are linked to more than one deposit…")
        try:
            user_addr_counts: Series | Any = self.deposit_user_df[  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType,]
                "user_address"
            ].value_counts()
            before = len(self.deposit_user_df)
            user_mask: DataFrame | Series | Any = self.deposit_user_df[  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType,]
                "user_address"
            ].isin(user_addr_counts.index[user_addr_counts < 2])  # pyright: ignore [reportArgumentType,reportUnknownMemberType]
            self.deposit_user_df = pd.DataFrame(self.deposit_user_df[user_mask])
            self.deposit_user_df = self.deposit_user_df.sort_values(  # pyright: ignore [reportUnknownMemberType]
                "deposit_address"
            ).reset_index(drop=True)
            self.logger.info(
                "Filtered %d rows; remaining %d",
                before - len(self.deposit_user_df),
                len(self.deposit_user_df),
            )
        except KeyError as e:
            raise SchemaMismatchError(
                "deposit_user_df is missing required columns",
                context={"required": ["user_address", "deposit_address"]},
                cause=e,
            )
        except Exception as e:
            raise DataParsingError(
                "Failed while filtering users with more than one deposit",
                cause=e,
            )

    def _filter_bot_users(self, receivers_from_user_limit: int = 100) -> None:
        self.logger.info(
            "Filtering likely bot users (threshold: %d receivers)…",
            receivers_from_user_limit,
        )
        try:
            if self.refresh_number_sent_received:
                _job = UpdateNumberSentReceivedJob(
                    addresses=self.deposit_user_df["user_address"].tolist(),  # pyright: ignore [reportUnknownMemberType,reportUnknownArgumentType]
                    chain_name=self.chain_name,
                    chain_id=self.chain_id,
                    arango=self.arango,
                    batch_size=self.batch_size,
                    max_workers=self.max_workers,
                )
                _user_documents = _job.run()
            else:
                _user_documents = self.arango.get_vertices_by_addresses(
                    self.deposit_user_df["user_address"].tolist(),  # pyright: ignore [reportUnknownMemberType,reportUnknownArgumentType]
                    batch_size=10000,
                )
        except AppError:
            raise
        except Exception as e:
            raise QueryError(
                "Failed to fetch user vertices from Arango",
                cause=e,
            )

        try:
            _user_receiver_map = {  # pyright: ignore [reportUnknownVariableType]
                doc["address"]: doc.get("numberSent", 0)  # pyright: ignore [reportUnknownMemberType]
                for doc in _user_documents  # pyright: ignore [reportUnknownVariableType,reportOptionalIterable,reportGeneralTypeIssues]
            }
        except Exception as e:
            raise DataParsingError(
                "Malformed user_documents returned from Arango",
                context={"records": len(_user_documents)},  # pyright: ignore [reportArgumentType]
                cause=e,
            )

        try:
            self.deposit_user_df["n_receivers"] = self.deposit_user_df[
                "user_address"
            ].map(lambda x: dict(_user_receiver_map).get(x, 0))  # pyright: ignore [reportUnknownMemberType,reportUnknownLambdaType,reportUnknownArgumentType]
            before = len(self.deposit_user_df)
            self.deposit_user_df = pd.DataFrame(
                self.deposit_user_df[
                    self.deposit_user_df["n_receivers"] < receivers_from_user_limit
                ]
            )
            self.deposit_user_df.drop(columns=["n_receivers"], inplace=True)
            self.deposit_user_df = self.deposit_user_df.sort_values(  # pyright: ignore [reportUnknownMemberType]
                "deposit_address"
            ).reset_index(drop=True)
            self.logger.info(
                "Removed %d suspected-bot rows; remaining %d",
                before - len(self.deposit_user_df),
                len(self.deposit_user_df),
            )
        except KeyError as e:
            raise SchemaMismatchError(
                "deposit_user_df missing required columns for bot filtering",
                context={"required": ["user_address"]},
                cause=e,
            )
        except Exception as e:
            raise DataParsingError(
                "Failed during bot user filtering computation",
                cause=e,
            )

    def _generate_pairs(self):
        """
        Instead of CSV -> upsert into Mongo (self.pairs_col).
        Also store chain/chainId and updatedAt for tracing.
        """
        self.logger.info(
            "Generating directed user pairs per deposit and writing to Mongo…"
        )
        if self.deposit_user_df.empty:
            self.logger.warning("No deposit-user data available to generate pairs")
            return

        # group deposit -> [users]
        try:
            deposit_user_mapping = (  # pyright: ignore [reportUnknownVariableType]
                self.deposit_user_df.groupby("deposit_address")["user_address"]  # pyright: ignore [reportUnknownMemberType]
                .agg(list)
                .reset_index()
            )
        except KeyError as e:
            raise SchemaMismatchError(
                "deposit_user_df missing required columns for pairing",
                context={"required": ["deposit_address", "user_address"]},
                cause=e,
            )
        except Exception as e:
            raise DataParsingError(
                "Failed to group deposit-user mapping",
                cause=e,
            )

        ops: list[UpdateOne] = []
        now = int(time.time())
        total_pairs = 0
        batch_idx = 0

        try:
            for _, row in deposit_user_mapping.iterrows():  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
                dep = row["deposit_address"]  # pyright: ignore [reportUnknownVariableType]
                users: list[str] = row["user_address"] or []  # pyright: ignore [reportAssignmentType,reportUnknownVariableType]

                # If a deposit is linked to a very large number of users,
                # permutations can explode quadratically; protect the system.
                if len(users) > 50000:
                    raise PerformanceError(
                        "Too many users attached to a single deposit; pairing would be excessive",
                        context={"deposit": dep, "user_count": len(users)},
                    )

                for x, y in permutations(users, 2):
                    key = {"X_address": x, "SubX_address": y, "chainId": self.chain_id}
                    doc = {  # pyright: ignore [reportUnknownVariableType]
                        **key,
                        "chain": self.chain_name,
                        "deposit_address": dep,
                        "updatedAt": now,
                    }
                    ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
                    total_pairs += 1
                    if len(ops) >= 1000:
                        try:
                            _ = self.pairs_col.bulk_write(ops, ordered=False)  # pyright: ignore [reportUnknownMemberType]
                        except Exception as e:
                            raise DatabaseWriteError(
                                "Failed bulk_write batch to Mongo",
                                context={"batch_index": batch_idx + 1, "ops": len(ops)},
                                cause=e,
                            )
                        batch_idx += 1
                        self.logger.info(
                            "bulk_write batch #%d: %d ops (running total pairs=%d)",
                            batch_idx,
                            1000,
                            total_pairs,
                        )
                        ops.clear()

            if ops:
                try:
                    _ = self.pairs_col.bulk_write(ops, ordered=False)  # pyright: ignore [reportUnknownMemberType]
                except Exception as e:
                    raise DatabaseWriteError(
                        "Failed final bulk_write to Mongo",
                        context={"final_ops": len(ops)},
                        cause=e,
                    )
                batch_idx += 1
                self.logger.info(
                    "bulk_write final batch #%d: %d ops (final total pairs=%d)",
                    batch_idx,
                    len(ops),
                    total_pairs,
                )

            self.logger.info(
                "Pairs upserted: %d into %s", total_pairs, self.pairs_col.name
            )

        except AppError:
            raise
        except Exception as e:
            raise ComputationError(
                "Unhandled error generating pairs",
                cause=e,
            )


class UpdateNumberSentReceivedJob(BaseJob):
    def __init__(
        self,
        addresses: list[str],
        chain_name: str,
        chain_id: str | None,
        arango: AddressGraphClient,
        batch_size: int = 10000,
        max_workers: int = 8,
    ):
        keys = [f"{chain_id}_{addr}" for addr in addresses]
        super().__init__(keys, batch_size, max_workers)  # pyright: ignore [reportUnknownMemberType]
        self.arango: AddressGraphClient = arango
        self.chain_name: str = chain_name
        self.addresses_with_number_sent_received: list[DepositData] = []
        self.logger: Logger = get_logger("UpdateNumberSentReceivedJob")

    @override
    def _execute_batch(self, works: list[str]):
        _time = int(time.time())
        self.logger.debug("Executing Arango update for %d addresses", len(works))
        _query = f"""
        FOR k in {works}
            LET id = CONCAT('{self.chain_name}_addresses/', k)
            LET n_to = (FOR v IN 1..1 OUTBOUND id
                GRAPH {self.chain_name}_transfers_graph
                COLLECT WITH COUNT INTO n
                RETURN n)
            LET n_from = (FOR v IN 1..1 INBOUND id
                GRAPH {self.chain_name}_transfers_graph
                COLLECT WITH COUNT INTO n
                RETURN n)
            UPDATE {{_key: k, numberSent: n_to[0], numberReceived: n_from[0], lastUpdatedAt:{_time} }}
                IN {self.chain_name}_addresses
            LET updated = NEW
            RETURN {{
                '_key': updated._key,
                'address': updated.address,
                'numberSent': updated.numberSent,
                'numberReceived': updated.numberReceived
            }}
        """
        try:
            new_data: Result[Cursor] = self.arango.aql(
                _query, batch_size=ARANGO_BATCH_SIZE
            )
            added = list(new_data)  # pyright: ignore [reportArgumentType]
            self.addresses_with_number_sent_received.extend(added)
            self.logger.debug("Updated+fetched %d rows from Arango", len(added))
        except TimeoutError as e:
            raise DatabaseTimeoutError(
                "Arango query timed out during numberSent/numberReceived update",
                cause=e,
            )
        except PermissionError as e:
            raise PermissionError(
                "Permission denied updating numberSent/numberReceived",
                cause=e,
            )
        except Exception as e:
            raise QueryError(
                "Arango query failed during numberSent/numberReceived update",
                cause=e,
            )

    @override
    def run(self) -> list[DepositData]:  # pyright: ignore [reportIncompatibleMethodOverride]
        self.logger.info("Running UpdateNumberSentReceivedJob…")
        t0 = time.time()
        try:
            super().run()
        except AppError:
            raise
        except Exception as e:
            raise ComputationError(
                "Unhandled error in UpdateNumberSentReceivedJob.run()",
                cause=e,
            )
        finally:
            self.logger.info(
                "Finished UpdateNumberSentReceivedJob: %d rows in %.2fs",
                len(self.addresses_with_number_sent_received),
                time.time() - t0,
            )
        return self.addresses_with_number_sent_received


class _GetUsersFromDepositsJob(BaseJob):
    def __init__(
        self,
        arango: AddressGraphClient,
        chain_id: str | None,
        chain_name: str,
        deposit_addresses: list[str],
        batch_size: int,
        max_workers: int,
    ):
        super().__init__(  # pyright: ignore [reportUnknownMemberType]
            work_iterable=deposit_addresses,
            batch_size=batch_size,
            max_workers=max_workers,
        )

        self.chain_id: str | None = chain_id
        self.chain_name: str = chain_name
        self.arango: AddressGraphClient = arango
        self.logger: Logger = get_logger("GetUsersFromDepositsJob")

        self._records: list[dict[str, str]] = list()

    @override
    def _execute_batch(self, works: list[str]):
        _deposit_address_ids: list[str] = [
            f"{self.chain_name}_addresses/{self.chain_id}_{addr}" for addr in works
        ]
        self.logger.debug("Querying users for %d deposits", len(works))
        _query: str = f"""
            FOR edge IN {self.chain_name}_transfers
            FILTER edge._to IN {_deposit_address_ids}
            RETURN {{
                'deposit_address': edge._to,
                'user_address': edge._from
            }}
        """
        try:
            returned_list = list(self.arango.aql(query=_query))
            _ = self._records.extend(returned_list)
        except TimeoutError as e:
            raise DatabaseTimeoutError(
                "Arango query timed out while fetching users from deposits", cause=e
            )
        except PermissionError as e:
            raise PermissionError(
                "Permission denied while fetching users from deposits", cause=e
            )
        except Exception as e:
            raise QueryError(
                "Arango query failed while fetching users from deposits", cause=e
            )

    @override
    def run(self) -> list[dict[str, str]]:  # pyright: ignore [reportIncompatibleMethodOverride]
        self.logger.info("Running GetUsersFromDepositsJob…")
        t0 = time.time()
        try:
            super().run()
        except AppError:
            raise
        except Exception as e:
            raise ComputationError(
                "Unhandled error in GetUsersFromDepositsJob.run()", cause=e
            )
        finally:
            self.logger.info(
                "Finished GetUsersFromDepositsJob: %d edges in %.2fs",
                len(self._records),
                time.time() - t0,
            )
        return self._records
