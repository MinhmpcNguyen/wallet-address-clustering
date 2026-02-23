import math
import os
import sys
from collections import defaultdict
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

# --- concurrency imports ---
from datetime import datetime, timezone
from itertools import chain
from logging import Logger
from threading import Lock
from typing import Any

import bson
import pymongo.errors as pymongo_errors
from arango.cursor import Cursor
from arango.database import StandardDatabase
from arango.result import Result
from bson.objectid import ObjectId
from databases.mongodb import MongoDB

# CHANGE THIS IMPORT PATH IF NEEDED:
from hooks.errors import (
    AppError,
    ConfigError,
    DatabaseConnectionError,
    DataParsingError,
    QueryError,
)
from multithread_processing.base_job import (  # pyright: ignore [reportMissingTypeStubs]
    BaseJob,
)
from pymongo import UpdateOne
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.database import Database
from schemas.deposit_schema import RawTokenFlowModel, SubgraphDoc
from typing_extensions import override
from utils.logger_utils import get_logger
from utils.time_utils import TimeUtils

sys.path.append(os.path.dirname(sys.path[0]))

logger: Logger = get_logger("TimeAmountExporter")

# ---------- constants ----------
MAX_BSON = 16 * 1024 * 1024
SAFETY = 1 * 1024 * 1024
BSON_SOFT_LIMIT = 12 * 1024 * 1024  # soft cap to prefer single-shot $in

# Re-used projections (avoid re-allocating dicts in hot paths)
PROJ_ID_ONLY = {"_id": 1}
PROJ_SUBGRAPH = {"_id": 1, "edges": 1, "vertices": 1, "chain": 1, "chainId": 1}

# -------- performance knobs (tunable, logic-preserving) --------
USE_SIMPLE_ID_SCAN_DEFAULT = True
GLOBAL_SORT_IDS_DEFAULT = True
ID_PARALLELISM_THRESHOLD = 1_000_000
PREPROCESS_PARALLEL_THRESHOLD = 200_000
PREPROCESS_CHUNK_SIZE = 100_000
MAX_IO_WORKERS_DEFAULT = 4
FLUSH_EVERY_DEFAULT = 5_000
INCLUDE_START_ID_ON_RESUME = (
    False  # keep semantics identical to "strictly >" by default
)
# USE_SIMPLE_ID_SCAN_DEFAULT = False
# GLOBAL_SORT_IDS_DEFAULT = True
# ID_PARALLELISM_THRESHOLD = 10
# PREPROCESS_PARALLEL_THRESHOLD = 20
# PREPROCESS_CHUNK_SIZE = 1000
# MAX_IO_WORKERS_DEFAULT = 4
# FLUSH_EVERY_DEFAULT = 1_000
# INCLUDE_START_ID_ON_RESUME = (
#     False  # keep semantics identical to "strictly >" by default
# )

# Bind hot symbols locally for micro-optimizations
_BSON_ENCODE = bson.BSON.encode


# ---------- logging helpers ----------
def _short(v, n=120):
    """Short pretty repr for logs."""
    try:
        s = repr(v)
    except Exception:
        s = str(v)
    return s if len(s) <= n else s[: n - 3] + "..."


def _pct(i, total):
    return f"{(100.0 * i / max(1, total)):.1f}%"


# ---------------------------
# Utilities (with validation)
# ---------------------------


def get_list(list_of_list: Iterable[Iterable[str]]) -> list[str]:
    """
    Flatten list of lists into a single list.
    """
    try:
        return list(chain(*list_of_list))
    except Exception as e:
        raise DataParsingError("Failed to flatten list-of-lists").with_cause(e)


def preprocess_pure_python(
    raw_list: list[RawTokenFlowModel], token_list: list[str]
) -> list[dict[str, str | float]]:
    """
    Pure-Python preprocessing with reduced temporary allocations.
    Semantics preserved:
      - For each (subgraphId, address, token_norm): sum(mean(valueInUSD per record))
      - time histogram computed per (subgraphId, address).
      - Invalid records are skipped (best-effort).
    """
    if logger.isEnabledFor(10):  # DEBUG
        logger.debug(
            "[PP] Preprocess chunk size=%d (token_list=%d)",
            len(raw_list),
            len(token_list),
        )

    try:
        agg: defaultdict[tuple[str, str, str], dict[str, Any]] = defaultdict(
            lambda: {"usd_sum": 0.0, "time_all": []}
        )

        token_set = set(token_list)  # O(1) membership
        bad = 0

        # Local fast bindings
        _chain_from_iterable = chain.from_iterable

        def _flatten(x):  # pyright: ignore [reportUnknownParameterType,reportMissingParameterType]
            """Flatten list or list-of-lists to a single list (keeps types)."""
            if not x:
                return []  # pyright: ignore [reportUnknownVariableType]
            if x and not isinstance(x[0], (list, tuple)):
                return list(x)  # pyright: ignore [reportUnknownVariableType,reportUnknownArgumentType]
            return list(_chain_from_iterable(x))  # pyright: ignore [reportUnknownVariableType,reportUnknownArgumentType]

        for rec in raw_list:
            try:
                sg = rec.subgraphId
                addr: str = rec.address
                tok: str = rec.token

                tok_norm: str = tok if tok in token_set else "other_token"

                times_f: list[str] = _flatten(rec.time)  # pyright: ignore [reportUnknownVariableType]
                usd_f: list[int | float] = _flatten(rec.valueInUSD)  # pyright: ignore [reportUnknownVariableType]

                mean_usd = (math.fsum(usd_f) / len(usd_f)) if usd_f else 0.0

                k = (sg, addr, tok_norm)
                a = agg[k]
                a["usd_sum"] += float(mean_usd)
                a["time_all"].extend(times_f)
            except Exception:
                bad += 1
                continue

        by_pair: defaultdict[tuple[str, str], dict[str, float]] = defaultdict(dict)
        time_bucket: defaultdict[tuple[str, str], list[str]] = defaultdict(list)

        for (sg, addr, tok_norm), vals in agg.items():
            by_pair[(sg, addr)][tok_norm] = float(vals["usd_sum"])
            time_bucket[(sg, addr)].extend(vals["time_all"])  # pyright: ignore [reportUnknownMemberType]

        docs: list[dict[str, Any]] = []
        _get_hist = TimeUtils.get_time_histogram
        for (sg, addr), token_sums in by_pair.items():  # pyright: ignore [reportUnknownVariableType]
            try:
                hist: list[int] = _get_hist(time_bucket[(sg, addr)])  # pyright: ignore [reportUnknownArgumentType]
            except Exception:
                continue

            # Build dict once, assign token fields directly (no extra dict.update)
            doc: dict[str, Any] = {"subgraphId": sg, "address": addr, "time": hist}
            for k, v in token_sums.items():
                doc[k] = v
            docs.append(doc)  # pyright: ignore [reportUnknownMemberType]

        if bad and logger.isEnabledFor(10):  # DEBUG
            logger.debug("[PP] Output docs=%d (skipped_bad=%d)", len(docs), bad)
        return docs  # pyright: ignore [reportUnknownVariableType]

    except AppError:
        raise
    except Exception as e:
        # Best-effort: return empty to keep pipeline going
        logger.warning("preprocess_pure_python failed; returning empty list: %s", e)
        return []


def _estimated_in_filter_size(num_ids: int) -> int:
    """
    Very rough upper-bound estimation of BSON size for {"_id": {"$in": ids}}.
    Uses ~28 bytes per ObjectId-like entry; safe enough to skip heavy encode calls.
    """
    # 16B header + per-field overhead — keep conservative headroom
    return 64 + num_ids * 28


def chunk_ids_by_bson(ids: Iterable[Any], field: str = "_id"):
    """
    Yield lists of ids such that {field: {"$in": batch}} stays < 16MB (minus SAFETY).
    Avoid repeated full BSON encodes by growing a small batch and checking only when needed.
    """
    batch: list[Any] = []
    append = batch.append

    for _id in ids:
        test_len = len(batch) + 1
        # Fast pre-check using estimation; fallback to encode near the soft boundary.
        if _estimated_in_filter_size(test_len) >= (MAX_BSON - SAFETY) // 2:
            # Now confirm with a real encode one time for safety.
            if _BSON_ENCODE({field: {"$in": batch + [_id]}}) >= (MAX_BSON - SAFETY):
                if not batch:
                    raise ValueError("Single _id makes command exceed BSON limit")
                yield batch
                batch = [_id]
                append = batch.append
                continue
        append(_id)
    if batch:
        yield batch


def _extract_id_list_from_filter(filt: dict[str, Any] | None) -> list[Any] | None:
    """
    If the filter is exactly/primarily an $in over _id (possibly wrapped),
    pull that list out so we can avoid sending it back to the server.
    """
    if not filt:
        return None
    _id_clause = filt.get("_id")
    if (
        isinstance(_id_clause, dict)
        and "$in" in _id_clause
        and isinstance(_id_clause["$in"], list)
    ):
        return list(_id_clause["$in"])
    return None


def _deep_extract_in_ids(filt: Any) -> list[Any] | None:
    """
    Recursively extract a large _id $in list from complex filters (e.g., with $and/$or nesting).
    """
    try:
        if not isinstance(filt, dict):
            return None
        if "_id" in filt and isinstance(filt["_id"], dict):
            in_clause = filt["_id"].get("$in")
            if isinstance(in_clause, list):
                return list(in_clause)
        for key in ("$and", "$or", "$nor"):
            if key in filt and isinstance(filt[key], list):
                for sub in filt[key]:
                    got = _deep_extract_in_ids(sub)
                    if got:
                        return got
        for v in filt.values():
            got = _deep_extract_in_ids(v)
            if got:
                return got
        return None
    except Exception:
        return None


# ---------------------------
# BSON size helpers (for chunking write payloads)
# ---------------------------


def _bson_size(doc: dict) -> int:
    """
    Safely estimate BSON-encoded size of a document.
    """
    try:
        return len(_BSON_ENCODE(doc))
    except Exception:
        return MAX_BSON


def _chunk_list(lst: list, chunk_len: int) -> list[list]:
    """
    Split list 'lst' into chunks of length 'chunk_len'.
    """
    return [lst[i : i + chunk_len] for i in range(0, len(lst), chunk_len)]


def _chunk_time_field_doc(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Skip documents that exceed the 16MB BSON limit instead of chunking.
    Return [doc] if small enough, [] if too large.
    """
    try:
        size = len(_BSON_ENCODE(doc))
        if size >= (16 * 1024 * 1024):  # hard 16MB limit
            if logger.isEnabledFor(20):  # INFO
                logger.warning(
                    "Skipping oversized document (%.2f MB): subgraphId=%s, address=%s",
                    size / (1024 * 1024),
                    doc.get("subgraphId"),
                    doc.get("address"),
                )
            return []
        else:
            d = dict(doc)
            d["isChunked"] = False
            d["chunkIndex"] = 0
            return [d]
    except Exception as e:
        logger.warning("Error encoding doc for size check: %s", e)
        return []


# ---------------------------
# Resilient bulk write helper (skip failing ops and continue)
# ---------------------------


def _resilient_bulk_flush(
    col: Collection[Any],
    ops: list[UpdateOne],
    *,
    ordered: bool = False,
    bypass_document_validation: bool = True,
) -> None:
    """
    Try to write all ops. On failures, separate good ops and retry; then fallback per-op.
    This function never raises; it logs and continues.
    """
    if not ops:
        return
    try:
        _ = col.bulk_write(
            ops,
            ordered=ordered,
            bypass_document_validation=bypass_document_validation,
        )
        return
    except pymongo_errors.BulkWriteError as e:
        write_errors = (e.details or {}).get("writeErrors", [])
        bad_idx = {we.get("index") for we in write_errors if "index" in we}
        good_ops = [op for i, op in enumerate(ops) if i not in bad_idx]

        if good_ops:
            try:
                _ = col.bulk_write(
                    good_ops,
                    ordered=ordered,
                    bypass_document_validation=bypass_document_validation,
                )
            except Exception as e2:
                logger.warning("Retry good_ops failed, fallback per-op: %s", e2)
        for op in ops:
            try:
                _ = col.bulk_write(
                    [op],
                    ordered=ordered,
                    bypass_document_validation=bypass_document_validation,
                )
            except Exception as e3:
                logger.warning("Skip 1 op due to error: %s", e3)
        return
    except Exception as e:
        logger.warning("bulk_write failed unexpectedly, fallback per-op: %s", e)
        for op in ops:
            try:
                _ = col.bulk_write(
                    [op],
                    ordered=ordered,
                    bypass_document_validation=bypass_document_validation,
                )
            except Exception as e4:
                logger.warning("Skip 1 op due to error: %s", e4)
        return


# ---------------------------
# Read helpers to avoid 16MB command limit
# ---------------------------


def safe_find_ids_with_huge_in(
    col: Collection[Any],
    base_filter: dict[str, Any],
    projection: dict[str, int] | None = None,
) -> list[Any]:
    """
    Perform multiple .find() calls by chunking a large _id $in list so that the
    command sent to MongoDB never exceeds the 16MB BSON limit.
    """
    if projection is None:
        projection = PROJ_ID_ONLY

    if not isinstance(base_filter, dict):
        base_filter = {}

    try:
        if base_filter and _bson_size(base_filter) >= (MAX_BSON - SAFETY):
            logger.warning(
                "safe_find_ids_with_huge_in: base_filter BSON too large; returning []"
            )
            return []
    except Exception:
        logger.warning(
            "safe_find_ids_with_huge_in: base_filter size check failed; returning []"
        )
        return []

    _id_clause = base_filter.get("_id") or {}
    in_ids = _id_clause.get("$in")
    if not isinstance(in_ids, list):
        try:
            return [d["_id"] for d in col.find(base_filter, projection)]
        except Exception as e:
            logger.exception(
                "safe_find_ids_with_huge_in: find failed; returning []. Reason: %s", e
            )
            return []

    ids: list[Any] = []
    base_without_ids = dict(base_filter)
    base_without_ids["_id"] = {}  # placeholder

    for id_batch in chunk_ids_by_bson(in_ids, field="_id"):
        try:
            filt = dict(base_without_ids)
            filt["_id"] = {"$in": id_batch}
            for d in col.find(filt, projection):
                ids.append(d["_id"])
        except pymongo_errors.DocumentTooLarge as e:
            logger.warning("Skip oversized filter batch (len=%d): %s", len(id_batch), e)
            continue
        except Exception as e:
            logger.exception("Unexpected error while loading ids for a batch: %s", e)
            continue
    return ids


# ---------------------------
# Parallel _id range split helpers for one-pass id scan
# ---------------------------


def _first_last_ids_only(col: Collection, log: Logger) -> tuple[Any | None, Any | None]:
    """
    Return min and max _id with projection {"_id":1}. If collection is empty, (None, None).
    """
    try:
        first = next(col.find({}, PROJ_ID_ONLY).sort([("_id", 1)]).limit(1), None)
        last = next(col.find({}, PROJ_ID_ONLY).sort([("_id", -1)]).limit(1), None)
        return (first["_id"] if first else None, last["_id"] if last else None)
    except Exception as e:
        log.warning(f"Failed to read first/last _id: {e}")
        return (None, None)


def _split_objectid_range(
    min_id: ObjectId, max_id: ObjectId, parts: int
) -> list[tuple[ObjectId, ObjectId | None]]:
    """
    Split ObjectId time range into `parts` half-open intervals.
    """
    if not (isinstance(min_id, ObjectId) and isinstance(max_id, ObjectId)):
        raise TypeError("_split_objectid_range requires ObjectId.")
    t0 = min_id.generation_time.timestamp()
    t1 = max_id.generation_time.timestamp()
    if t1 <= t0 or parts <= 1:
        return [(min_id, None)]
    dt = (t1 - t0) / parts
    out: list[tuple[ObjectId, ObjectId | None]] = []
    for i in range(parts):
        start_ts = int(t0 + i * dt)
        end_ts = int(t0 + (i + 1) * dt)
        start_id = ObjectId.from_datetime(datetime.utcfromtimestamp(start_ts))
        if i == parts - 1:
            out.append((start_id, None))
        else:
            end_id = ObjectId.from_datetime(datetime.utcfromtimestamp(end_ts))
            out.append((start_id, end_id))
    out[0] = (min_id, out[0][1])
    return out


def _split_generic_sorted_ranges(
    col: Collection,
    parts: int,
    log: Logger,
) -> list[tuple[Any, Any | None]]:
    """
    Generic splitter that builds `parts` _id ranges using $bucketAuto; fallback to probed keys.
    Works for str/int/date/UUID/etc.
    """
    parts = max(1, parts)
    try:
        pipeline = [{"$bucketAuto": {"groupBy": "$_id", "buckets": parts}}]
        buckets = list(col.aggregate(pipeline, allowDiskUse=True))
        ranges: list[tuple[Any, Any | None]] = []
        for i, b in enumerate(buckets):
            lo, hi = b.get("min"), b.get("max")
            ranges.append((lo, None if i == len(buckets) - 1 else hi))
        try:
            first = next(col.find({}, PROJ_ID_ONLY).sort([("_id", 1)]).limit(1), None)
            if first and ranges:
                ranges[0] = (first["_id"], ranges[0][1])
        except Exception:
            pass
        if ranges:
            return ranges
    except Exception as e:
        log.debug(f"$bucketAuto failed, fallback to probed keys: {e}")

    try:
        total = col.estimated_document_count()
        if total == 0:
            return []
        split_keys: list[Any] = []
        for i in range(parts):
            pos = int((total * i) / parts)
            doc = next(
                col.find({}, PROJ_ID_ONLY).sort([("_id", 1)]).skip(pos).limit(1), None
            )
            if doc:
                split_keys.append(doc["_id"])
        ranges = []
        for i in range(len(split_keys)):
            lo = split_keys[i]
            hi = split_keys[i + 1] if i + 1 < len(split_keys) else None
            ranges.append((lo, hi))
        return ranges or [(None, None)]
    except Exception as e:
        log.warning(f"Fallback probing split keys failed: {e}")
        first = next(col.find({}, PROJ_ID_ONLY).sort([("_id", 1)]).limit(1), None)
        return [(first["_id"], None)] if first else []


# ---------------------------
# Main job (optimized, logic-preserving)
# ---------------------------


class TimeAmountExporterJob(BaseJob):
    def __init__(
        self,
        chain: str,
        chain_id: str,
        list_index: list[Any] | None,
        token_list: list[str],
        transaction_database: StandardDatabase,
        mongo_db: MongoDB,
        mongo_collection_prefix: str,
        subgraph_collection_name: str,
        mongo_query_filter: dict[str, dict[str, list[str]]] | None,
        max_workers: int = 4,
        batch_size: int = 1,
        flush_every: int = FLUSH_EVERY_DEFAULT,
        # --- checkpoint + ranges ---
        job_key: str | None = None,
        resume: bool = True,
        start_id: Any | None = None,  # keep native type
        end_id: Any | None = None,
        total_limit: int | None = 1000,
        # --- performance flags ---
        use_simple_id_scan: bool = USE_SIMPLE_ID_SCAN_DEFAULT,
        global_sort_ids: bool = GLOBAL_SORT_IDS_DEFAULT,
        id_parallelism_threshold: int = ID_PARALLELISM_THRESHOLD,
        preprocess_parallel_threshold: int = PREPROCESS_PARALLEL_THRESHOLD,
        preprocess_chunk_size: int = PREPROCESS_CHUNK_SIZE,
        max_io_workers: int = MAX_IO_WORKERS_DEFAULT,
        single_in_bson_soft_limit: int = BSON_SOFT_LIMIT,
        include_start_id_on_resume: bool = INCLUDE_START_ID_ON_RESUME,
    ):
        try:
            if not chain or not chain_id:
                raise ConfigError("chain and chain_id must be provided")

            self.EdgeQuery: set[str] = set()
            self.token_list: list[str] = token_list
            self.chain: str = chain
            self.chain_id: str = chain_id

            self.transaction_database: StandardDatabase = transaction_database
            self.batch_size: int = batch_size
            self.max_workers: int = max_workers

            # Mongo collections
            self.mongo_db: MongoDB = mongo_db
            self.db: Database[Any] = mongo_db.db
            self.subgraph_col: Collection[SubgraphDoc] = self.db[
                subgraph_collection_name
            ]
            self.col_subgraph = self.db[f"{mongo_collection_prefix}_subgraph"]  # pyright: ignore [reportUnannotatedClassAttribute]
            self.col_from = self.db[f"{mongo_collection_prefix}_from"]  # pyright: ignore [reportUnannotatedClassAttribute]
            self.col_to = self.db[f"{mongo_collection_prefix}_to"]  # pyright: ignore [reportUnannotatedClassAttribute]
            self.col_progress = self.db[f"{mongo_collection_prefix}_progress"]  # pyright: ignore [reportUnannotatedClassAttribute]
            self.mongo_query_filter: dict[str, dict[str, list[str]]] | None = (
                mongo_query_filter or {}
            )

            # Buffers
            self.buffer_subgraph: list[SubgraphDoc] = []
            self.buffer_from_raw: list[RawTokenFlowModel] = []
            self.buffer_to_raw: list[RawTokenFlowModel] = []

            # Streaming flush control
            self.flush_every: int = max(1, flush_every)
            self._since_last_flush: int = 0

            # Checkpoint + range
            self.job_key = job_key or f"{self.chain}:{self.subgraph_col.name}"
            self.resume = resume
            self.start_id = start_id
            self.end_id = end_id
            self.total_limit = total_limit
            self._processed_total: int = 0
            self._last_processed_id: Any | None = None
            self.include_start_id_on_resume = include_start_id_on_resume

            # Perf params
            self.use_simple_id_scan = use_simple_id_scan
            self.global_sort_ids = global_sort_ids
            self.id_parallelism_threshold = id_parallelism_threshold
            self.preprocess_parallel_threshold = preprocess_parallel_threshold
            self.preprocess_chunk_size = preprocess_chunk_size
            self.max_io_workers = max_io_workers
            self.single_in_bson_soft_limit = single_in_bson_soft_limit

            # Decide work_iterable once
            ids_for_work: list[Any]
            if list_index:
                ids_for_work = list_index
            else:
                provided_ids = _deep_extract_in_ids(
                    self.mongo_query_filter
                ) or _extract_id_list_from_filter(self.mongo_query_filter)
                if provided_ids:
                    ids_for_work = provided_ids
                    self.mongo_query_filter = {}
                else:
                    est_total = self.subgraph_col.estimated_document_count()
                    if self.use_simple_id_scan or (
                        est_total < self.id_parallelism_threshold
                    ):
                        ids_for_work = [
                            d["_id"] for d in self.subgraph_col.find({}, PROJ_ID_ONLY)
                        ]
                        if self.global_sort_ids:
                            try:
                                ids_for_work.sort()
                            except Exception:
                                pass
                    else:
                        ids_for_work = self._load_ids_parallel_once(
                            total_limit=None,
                            parallelism=self.max_workers,
                            read_batch_docs=5000,
                            find_batch_size=1000,
                            max_resume_retries=10,
                            global_sort=self.global_sort_ids,
                        )
                        if not ids_for_work and logger.isEnabledFor(20):  # INFO
                            logger.warning(
                                "No subgraph ids found. Proceeding with empty workload."
                            )

            # Resume from checkpoint
            if self.resume:
                try:
                    cp = self.col_progress.find_one(
                        {"_id": self.job_key}, {"last_processed_id": 1}
                    )
                    if cp and cp.get("last_processed_id") is not None:
                        if (self.start_id is None) or (
                            cp["last_processed_id"] >= self.start_id
                        ):
                            self.start_id = cp["last_processed_id"]
                            logger.info(
                                "[Resume] Loaded last_processed_id=%s",
                                _short(self.start_id),
                            )
                except Exception as e:
                    logger.warning("[Resume] Failed to read checkpoint: %s", e)

            # Ensure stable order if asked
            if self.global_sort_ids:
                try:
                    ids_for_work.sort()
                except Exception:
                    pass

            # Apply range & limit
            ids_global = ids_for_work
            if self.start_id is not None:
                if self.include_start_id_on_resume:
                    ids_global = [i for i in ids_global if i >= self.start_id]
                else:
                    ids_global = [i for i in ids_global if i > self.start_id]
            if self.end_id is not None:
                ids_global = [i for i in ids_global if i < self.end_id]
            if self.total_limit is not None and self.total_limit >= 0:
                ids_global = ids_global[: self.total_limit]

            logger.info(
                "[Init] After range/resume filter: %d id(s). start_id=%s end_id=%s limit=%s",
                len(ids_global),
                _short(self.start_id),
                _short(self.end_id),
                str(self.total_limit),
            )
            if ids_global[:1] and logger.isEnabledFor(10):  # DEBUG
                logger.debug(
                    "[Init] First id type=%s value=%s",
                    type(ids_global[0]).__name__,
                    _short(ids_global[0]),
                )

            super().__init__(  # pyright: ignore [reportUnknownMemberType]
                work_iterable=ids_global,  # type: list[str]
                max_workers=max_workers,
                batch_size=batch_size,
            )

        except AppError:
            raise
        except Exception as e:
            logger.exception(
                "Failed to initialize TimeAmountExporterJob; continuing with empty workload: %s",
                e,
            )
            super().__init__(  # pyright: ignore [reportUnknownMemberType]
                work_iterable=[],
                max_workers=max_workers if "max_workers" in locals() else 1,
                batch_size=batch_size if "batch_size" in locals() else 1,
            )

    # ---------- Parallel ID scanner (one pass, internal) ----------
    def _load_ids_parallel_once(
        self,
        *,
        total_limit: int | None = None,
        parallelism: int = 4,
        read_batch_docs: int = 5000,
        find_batch_size: int = 1000,
        max_resume_retries: int = 10,
        global_sort: bool = False,
    ) -> list[Any]:
        """
        One-pass parallel scan to collect ONLY _id from self.subgraph_col.
        """
        log = get_logger(f"parallel id scan: {self.subgraph_col.name}")
        proj = PROJ_ID_ONLY

        log.info(
            "[ID-Scan] Begin parallel id scan on '%s' (parallelism=%d, read_batch_docs=%d, find_batch_size=%d)",
            self.subgraph_col.name,
            parallelism,
            read_batch_docs,
            find_batch_size,
        )

        min_id, max_id = _first_last_ids_only(self.subgraph_col, log)
        if min_id is None or max_id is None:
            return []

        log.info("[ID-Scan] Min _id=%s | Max _id=%s", _short(min_id), _short(max_id))

        parts = max(1, parallelism)
        if isinstance(min_id, ObjectId) and isinstance(max_id, ObjectId):
            slices = _split_objectid_range(min_id, max_id, parts)
        else:
            slices = _split_generic_sorted_ranges(self.subgraph_col, parts, log)
        if not slices:
            slices = [(min_id, None)]

        ids: list[Any] = []
        lock = Lock()
        remaining = [total_limit if total_limit is not None else float("inf")]

        def _iter_in_batches_ids(iterable, batch_size: int):
            batch = []
            for item in iterable:
                batch.append(item)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        def _iter_with_resume_range_ids(low_id, high_id):
            last_id: Any | None = None
            finished = False
            retries = 0
            while not finished:
                flt: dict[str, Any] = {}
                if low_id is not None and high_id is not None:
                    flt["_id"] = {"$gte": low_id, "$lt": high_id}
                elif low_id is not None:
                    flt["_id"] = {"$gte": low_id}
                elif high_id is not None:
                    flt["_id"] = {"$lt": high_id}
                if last_id is not None:
                    if "_id" in flt:
                        cur = flt["_id"]
                        cur["$gt"] = last_id
                        cur.pop("$gte", None)
                    else:
                        flt["_id"] = {"$gt": last_id}
                try:
                    cursor = self.subgraph_col.find(
                        flt,
                        proj,
                        sort=[("_id", 1)],
                        batch_size=find_batch_size,
                        no_cursor_timeout=True,
                        max_time_ms=0,
                        hint=[("_id", 1)],
                    )
                except Exception as e:
                    raise DatabaseConnectionError(
                        f"Failed to open ranged cursor: {e}"
                    ) from e

                try:
                    for logical in _iter_in_batches_ids(cursor, read_batch_docs):
                        last_id = logical[-1].get("_id", last_id)
                        yield [d["_id"] for d in logical]
                    finished = True
                except (pymongo_errors.OperationFailure,) as e:
                    log.warning(f"OperationFailure in id scan, retry: {e}")
                    retries += 1
                    if retries > max_resume_retries:
                        raise QueryError("Exceeded max resume retries") from e
                    import time as _t

                    _t.sleep(min(2.0, 0.2 * retries))
                    continue
                except (
                    pymongo_errors.PyMongoError,
                    pymongo_errors.CursorNotFound,
                ) as e:
                    log.warning(f"PyMongoError in id scan, retry: {e}")
                    retries += 1
                    if retries > max_resume_retries:
                        raise QueryError("Exceeded max resume retries") from e
                    import time as _t

                    _t.sleep(min(2.0, 0.2 * retries))
                    continue
                except Exception as e:
                    log.warning(f"Unknown id scan error, retry: {e}")
                    retries += 1
                    if retries > max_resume_retries:
                        raise QueryError("Exceeded max resume retries") from e
                    import time as _t

                    _t.sleep(min(2.0, 0.2 * retries))
                    continue
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        pass

        def worker(lo, hi) -> list[Any]:
            local: list[Any] = []
            for logical_ids in _iter_with_resume_range_ids(lo, hi):
                with lock:
                    if remaining[0] <= 0:
                        break
                    take = (
                        len(logical_ids)
                        if remaining[0] == float("inf")
                        else int(min(len(logical_ids), remaining[0]))
                    )
                    if remaining[0] != float("inf"):
                        remaining[0] -= take
                if take > 0:
                    local.extend([_id for _id in logical_ids[:take]])
                if remaining[0] == 0:
                    break
            return local

        with ThreadPoolExecutor(max_workers=len(slices)) as tp:
            futs = [tp.submit(worker, lo, hi) for (lo, hi) in slices]
            for f in as_completed(futs):
                chunk = f.result()
                if chunk:
                    ids.extend(chunk)

        if global_sort:
            try:
                ids.sort()
            except Exception:
                pass

        log.info("[ID-Scan] Completed. Total ids collected: %d", len(ids))
        return ids  # type: ignore[return-value]

    # ---------------------------
    # Parallel Mongo read (I/O) over id batches
    # ---------------------------
    def _fetch_subgraphs_by_ids(self, ids: list[Any]) -> dict[Any, dict[str, Any]]:
        """
        Get subgraph documents by _id → dict: {_id: doc}
        Adaptive strategy:
          - One-shot $in if estimated filter size < soft limit,
          - Otherwise chunk + ThreadPool (bounded to number of batches).
        """
        out: dict[Any, dict[str, Any]] = {}
        if not ids:
            return out  # pyright: ignore [reportUnknownVariableType]

        # Quick estimate to avoid heavy BSON encodes when safe
        if _estimated_in_filter_size(len(ids)) < self.single_in_bson_soft_limit:
            try:
                for d in self.subgraph_col.find(
                    {"_id": {"$in": ids}}, PROJ_SUBGRAPH, batch_size=2048
                ):
                    out[d["_id"]] = d  # pyright: ignore
                return out
            except Exception:
                # fallback to chunked path below
                pass

        skipped: list[Any] = []
        batches = list(chunk_ids_by_bson(ids, field="_id"))

        def _fetch_one_batch(id_batch: list[Any]) -> dict[Any, dict[str, Any]]:
            got: dict[Any, dict[str, Any]] = {}
            try:
                cursor = self.subgraph_col.find(
                    {"_id": {"$in": id_batch}}, PROJ_SUBGRAPH, batch_size=1024
                )
                for d in cursor:
                    got[d["_id"]] = d
            except (pymongo_errors.DocumentTooLarge, pymongo_errors.InvalidBSON):
                # Fallback per-id
                for _id in id_batch:
                    try:
                        doc = self.subgraph_col.find_one({"_id": _id}, PROJ_SUBGRAPH)
                        if doc is not None:
                            got[doc["_id"]] = doc
                    except Exception:
                        skipped.append(_id)
            except Exception:
                # Fallback per-id on generic errors
                for _id in id_batch:
                    try:
                        doc = self.subgraph_col.find_one({"_id": _id}, PROJ_SUBGRAPH)
                        if doc is not None:
                            got[doc["_id"]] = doc
                    except Exception:
                        skipped.append(_id)
            return got

        max_workers = min(max(1, self.max_io_workers), len(batches))
        if max_workers == 1 and len(batches) == 1:
            out.update(_fetch_one_batch(batches[0]))
            return out

        with ThreadPoolExecutor(max_workers=max_workers) as tp:
            futs = [tp.submit(_fetch_one_batch, b) for b in batches]
            for f in as_completed(futs):
                try:
                    out.update(f.result())
                except Exception as e:
                    logger.exception("Fetch batch failed and was skipped: %s", e)

        if skipped and logger.isEnabledFor(20):  # INFO
            logger.info(
                "[Fetch] Skipped %d id(s) due to doc issues; sample=%s",
                len(skipped),
                _short(skipped[:5]),
            )
        return out

    # ---------------------------
    # Checkpoint helper
    # ---------------------------
    def _save_checkpoint(self, tag: str = "auto") -> None:
        """
        Upsert a small progress document so we can resume later.
        """
        try:
            doc = {
                "_id": self.job_key,
                "last_processed_id": self._last_processed_id,
                "processed_total": self._processed_total,
                "updatedAt": datetime.now(timezone.utc),
                "tag": tag,
                "subgraph_collection": self.subgraph_col.name,
                "chain": self.chain,
                "chainId": self.chain_id,
                "flush_every": self.flush_every,
            }
            self.col_progress.update_one(
                {"_id": self.job_key}, {"$set": doc}, upsert=True
            )
            if logger.isEnabledFor(20):  # INFO
                logger.info(
                    "[Checkpoint] job=%s tag=%s last_id=%s processed_total=%d",
                    self.job_key,
                    tag,
                    _short(self._last_processed_id),
                    self._processed_total,
                )
        except Exception as e:
            logger.warning("[Checkpoint] Failed to save progress: %s", e)

    # ---------------------------
    # Streaming flush phase (checkpoint)
    # ---------------------------
    def _flush_phase(self, *, final: bool = False) -> None:
        """
        One checkpoint: write subgraph docs, preprocess FROM/TO, write to Mongo, clear buffers.
        """
        tag = "Final" if final else "Flush"
        if logger.isEnabledFor(20):  # INFO
            logger.info(
                "[%s] Begin. Buffers — subgraph=%d, from_raw=%d, to_raw=%d",
                tag,
                len(self.buffer_subgraph),
                len(self.buffer_from_raw),
                len(self.buffer_to_raw),
            )

        # (1) Write subgraph buffer
        try:
            if self.buffer_subgraph:
                ops = [
                    UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True)
                    for d in self.buffer_subgraph
                ]
                _resilient_bulk_flush(self.col_subgraph, ops, ordered=False)
        except Exception as e:
            logger.exception("[%s] subgraph write error (best-effort): %s", tag, e)

        # (2) CPU preprocess (adaptive)
        try:
            total_raw = len(self.buffer_from_raw) + len(self.buffer_to_raw)
            if total_raw < self.preprocess_parallel_threshold:
                from_docs = preprocess_pure_python(
                    self.buffer_from_raw, self.token_list
                )
                to_docs = preprocess_pure_python(self.buffer_to_raw, self.token_list)
            else:
                from_docs = self._preprocess_parallel(
                    self.buffer_from_raw, self.token_list
                )
                to_docs = self._preprocess_parallel(self.buffer_to_raw, self.token_list)
        except Exception as e:
            logger.exception("[%s] preprocess failed; continue best-effort: %s", tag, e)
            from_docs, to_docs = [], []

        # (3) Size-check/chunking (logic-preserving)
        from_docs_chunked: list[dict[str, Any]] = []
        to_docs_chunked: list[dict[str, Any]] = []
        for d in from_docs:
            from_docs_chunked.extend(_chunk_time_field_doc(d))
        for d in to_docs:
            to_docs_chunked.extend(_chunk_time_field_doc(d))

        # (4) Build ops
        ops_from = [
            UpdateOne(
                {
                    "subgraphId": d["subgraphId"],
                    "address": d["address"],
                    "chunkIndex": d.get("chunkIndex", 0),
                },
                {"$set": d},
                upsert=True,
            )
            for d in from_docs_chunked
        ]
        ops_to = [
            UpdateOne(
                {
                    "subgraphId": d["subgraphId"],
                    "address": d["address"],
                    "chunkIndex": d.get("chunkIndex", 0),
                },
                {"$set": d},
                upsert=True,
            )
            for d in to_docs_chunked
        ]

        # (5) Parallel flush two collections
        def _flush(col: Collection[Any], ops: list[UpdateOne]) -> None:
            if ops:
                _resilient_bulk_flush(col, ops, ordered=False)

        try:
            with ThreadPoolExecutor(max_workers=2) as tp:
                futs = [
                    tp.submit(_flush, self.col_from, ops_from),
                    tp.submit(_flush, self.col_to, ops_to),
                ]
                for f in as_completed(futs):
                    _ = f.result()
        except Exception as e:
            logger.exception("[%s] parallel write stage error: %s", tag, e)

        if logger.isEnabledFor(20):  # INFO
            logger.info(
                "[%s] Done. Wrote: from_ops=%d, to_ops=%d",
                tag,
                len(ops_from),
                len(ops_to),
            )

        # (6) Clear buffers
        self.buffer_subgraph.clear()
        self.buffer_from_raw.clear()
        self.buffer_to_raw.clear()
        self._since_last_flush = 0

        # (7) Save checkpoint
        self._save_checkpoint(tag=tag)

    def _preprocess_parallel(
        self,
        raw: list[RawTokenFlowModel],
        token_list: list[str],
        chunk_size: int | None = None,
        max_workers: int | None = None,
    ) -> list[dict[str, str | float]]:
        """
        Split raw buffer into large chunks and process them in a ProcessPool.
        """
        chunk_size = chunk_size or self.preprocess_chunk_size
        if logger.isEnabledFor(20):  # INFO
            logger.info(
                "[Preprocess] Begin CPU preprocess: items=%d, chunk_size=%d",
                len(raw),
                chunk_size,
            )
        if not raw:
            return []
        max_workers = max_workers or (os.cpu_count() or 4)

        def _chunks(lst: list[Any], n: int):
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        out: list[dict[str, str | float]] = []
        with ProcessPoolExecutor(max_workers=max_workers) as pe:
            futs = [
                pe.submit(preprocess_pure_python, part, token_list)
                for part in _chunks(raw, chunk_size)
            ]
            for f in as_completed(futs):
                try:
                    out.extend(f.result())
                except Exception as e:
                    logger.exception("Skip failed preprocess chunk: %s", e)
                    continue
        if logger.isEnabledFor(20):  # INFO
            logger.info("[Preprocess] Completed. Output docs: %d", len(out))
        return out

    # ---------------------------
    # Batch execution
    # ---------------------------
    @override
    def _execute_batch(self, works: list[Any]):
        logger.info("[Batch] Start processing batch: %d id(s)", len(works))
        try:
            id_to_doc = self._fetch_subgraphs_by_ids(works)
        except AppError as e:
            logger.exception("Skip batch of %d ids due to AppError: %s", len(works), e)
            return
        except Exception as e:
            logger.exception("Skip batch of %d ids due to error: %s", len(works), e)
            return

        fetched = len(id_to_doc)
        if fetched == 0:
            logger.warning("[Batch] No docs fetched for this batch.")
        else:
            logger.info("[Batch] Fetched %d/%d doc(s)", fetched, len(works))

        append_from = self.buffer_from_raw.append
        append_to = self.buffer_to_raw.append
        edge_query_add = self.EdgeQuery.add

        for subgraph_id in works:
            doc = id_to_doc.get(subgraph_id)
            if not doc:
                continue

            subgraph_doc = SubgraphDoc(
                _id=subgraph_id,
                vertices=[],
                chain=self.chain,
                chainId=self.chain_id,
            )

            edges_field: list[Any] | str = doc.get("edges") or []
            if not isinstance(edges_field, list):
                edges_field = []

            edges_set = {
                f"{self.chain}_transfers/{self.chain_id}_{item.get('from')}_{item.get('to')}"
                for item in edges_field
                if item and item.get("from") and item.get("to")
            }
            edges: list[str] = list(edges_set - self.EdgeQuery)

            if not edges:
                self.buffer_subgraph.append(subgraph_doc)
                continue

            # Arango query
            query = f"""
                FOR doc IN {self.chain}_transfers
                FILTER doc._id IN @edges
                RETURN doc
            """
            try:
                results: Result[Cursor] = self.transaction_database.aql.execute(
                    query, bind_vars={"edges": edges}, batch_size=1000
                )
            except Exception as e:
                logger.error("Arango query error for subgraph %s: %s", subgraph_id, e)
                continue

            # Parse each result doc
            try:
                for a in results:  # pyright: ignore [reportUnknownVariableType,reportGeneralTypeIssues,reportOptionalIterable]
                    try:
                        edge_query_add(a["_id"])  # pyright: ignore [reportUnknownArgumentType]
                        parts: list[str] = str(a["_key"]).split("_")  # pyright: ignore [reportUnknownArgumentType]
                        if len(parts) < 3:
                            continue

                        from_addr: str = parts[1]
                        to_addr: str = parts[2]
                        _ = subgraph_doc["vertices"].append(
                            {"_from": from_addr, "_to": to_addr}
                        )

                        ttl = a.get("tokenTransferLogs") or {}  # pyright: ignore [reportUnknownVariableType,reportUnknownMemberType]
                        if not isinstance(ttl, dict):
                            continue

                        # Single pass over token -> time_data
                        for token, time_data in ttl.items():  # pyright: ignore [reportUnknownVariableType]
                            if not isinstance(time_data, dict):
                                continue

                            # Filter valid (t, vals) pairs once
                            buf = [
                                (t, vals)
                                for t, vals in time_data.items()
                                if isinstance(vals, dict)
                                and vals.get("valueInUSD") is not None
                            ]
                            if not buf:
                                continue

                            n = len(buf)
                            times = [None] * n
                            amounts = [None] * n
                            values_usd = [None] * n
                            for i, (t, v) in enumerate(buf):
                                times[i] = t
                                amounts[i] = v.get("amount")
                                values_usd[i] = v["valueInUSD"]

                            rec = RawTokenFlowModel(
                                subgraphId=subgraph_id,
                                address=from_addr,
                                token=token,  # pyright: ignore [reportUnknownArgumentType]
                                time=times,
                                amount=amounts,
                                valueInUSD=values_usd,
                            )
                            append_from(rec)
                            append_to(rec)
                    except Exception as e:
                        # Skip this edge only; keep parsing others
                        if logger.isEnabledFor(10):  # DEBUG
                            logger.debug(
                                "Skip edge due to parse error in subgraph %s: %s",
                                subgraph_id,
                                e,
                            )
                        continue

            except Exception as e:
                logger.warning(
                    "Skip subgraph %s due to major parse error: %s", subgraph_id, e
                )
                continue

            # Append once per subgraph
            self.buffer_subgraph.append(subgraph_doc)

        # --- Update counters for checkpointing ---
        self._processed_total += len(works)
        try:
            last_id_batch = max(works) if works else None
            if last_id_batch and (
                self._last_processed_id is None
                or last_id_batch > self._last_processed_id
            ):
                self._last_processed_id = last_id_batch
        except Exception:
            pass

        # Streaming window
        self._since_last_flush += len(works)
        if self._since_last_flush >= self.flush_every:
            logger.info(
                "[Flush] Threshold reached (%d >= %d). Flushing...",
                self._since_last_flush,
                self.flush_every,
            )
            self._flush_phase(final=False)

    # ---------------------------
    # End: only flush remaining buffers
    # ---------------------------
    @override
    def _end(self):
        super()._end()
        if logger.isEnabledFor(20):  # INFO
            logger.info(
                "[End] Finalize. Remaining buffers — subgraph=%d, from_raw=%d, to_raw=%d",
                len(self.buffer_subgraph),
                len(self.buffer_from_raw),
                len(self.buffer_to_raw),
            )
        self._flush_phase(final=True)
        logger.info("Successfully finished inserting all collections (best-effort)")
        logger.info("[End] All collections flush attempted (best-effort).")
