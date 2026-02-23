import datetime
import os
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Dict, Iterable, Optional

from bson.objectid import ObjectId

# Keep sys.path mutation as in your codebase
sys.path.append(os.path.dirname(sys.path[0]))

# --- near-top additions / imports ---
import random
import time
from typing import cast

import bson  # for safe BSON size estimation
import pymongo.errors as pymongo_errors
from databases.mongodb import MongoDB
from hooks.errors import (
    ComputationError,
    DatabaseConnectionError,
    DatabaseWriteError,
    QueryError,
    SchemaMismatchError,
)
from pymongo import UpdateOne
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.database import Database
from pymongo.write_concern import WriteConcern
from schemas.subgraph_schema import OutSubgraphDoc, SubgraphDoc
from utils.logger_utils import get_logger

# ------------------------------------
# Resilient bulk-write + byte-aware split
# ------------------------------------

# Keep a wide margin under the 16MB BSON limit for a single wire message.
# This budget is applied to the sum of estimated UpdateOne payloads we send at once.
_BULK_WIRE_BUDGET = 8 * 1024 * 1024  # ~8MB


def _estimate_update_one_bytes(op: UpdateOne) -> int:
    """
    Rough estimate of a single UpdateOne's encoded size by BSON-encoding
    a minimal element that mirrors the server's 'updates' array entry.
    This is conservative and only used for chunk planning.
    """
    try:
        sample = {
            "q": op._filter,  # pyright: ignore [reportPrivateUsage]
            "u": op._doc,  # pyright: ignore [reportPrivateUsage]
            "multi": False,
            "upsert": bool(getattr(op, "_upsert", False)),  # pyright: ignore [reportPrivateUsage]
        }
        return len(bson.BSON.encode(sample))
    except Exception:
        # If anything goes wrong, assume large to force smaller chunks.
        return 128 * 1024


def _split_ops_by_bytes(
    ops: list[UpdateOne], budget_bytes: int
) -> list[list[UpdateOne]]:
    """
    Split UpdateOne ops into sublists whose estimated total size stays under 'budget_bytes'.
    Preserves input order. If a single op exceeds the budget, it is sent alone.
    """
    out: list[list[UpdateOne]] = []
    cur: list[UpdateOne] = []
    cur_bytes = 0

    for op in ops:
        est = _estimate_update_one_bytes(op)
        if est >= budget_bytes:
            if cur:
                out.append(cur)
                cur, cur_bytes = [], 0
            out.append([op])
            continue
        if cur and (cur_bytes + est > budget_bytes):
            out.append(cur)
            cur, cur_bytes = [op], est
        else:
            cur.append(op)
            cur_bytes += est

    if cur:
        out.append(cur)
    return out


def _resilient_bulk_write(
    col,
    ops,
    *,
    ordered: bool = False,
    bypass_document_validation: bool = True,
    max_retries: int = 8,
    initial_chunk: int = 300,
    min_chunk: int = 25,
    base_sleep: float = 0.35,
) -> None:
    """
    Perform bulk_write with:
      - Light write concern (w=1, j=False) to reduce server-side latency.
      - Pre-split by approximate wire size to stay well under 16MB.
      - Retry/backoff on transient failures (including operation cancelled).
      - Adaptive chunk shrink by count on retry.
    Semantics unchanged: same upsert effects, just more robust delivery.
    """
    if not ops:
        return

    # First level: split by *bytes* to keep each wire message comfortably small.
    byte_chunks = _split_ops_by_bytes(ops, budget_bytes=_BULK_WIRE_BUDGET)

    # Use a lighter write concern to avoid unnecessary fsync blocking.
    wc_col = col.with_options(write_concern=WriteConcern(w=1, j=False))

    for byte_chunk in byte_chunks:
        start = 0
        n = len(byte_chunk)
        chunk = max(min_chunk, min(initial_chunk, n))

        while start < n:
            end = min(n, start + chunk)
            subops = byte_chunk[start:end]
            attempt = 0

            while True:
                try:
                    _ = wc_col.bulk_write(  # pyright: ignore [reportUnknownMemberType]
                        subops,
                        ordered=ordered,
                        bypass_document_validation=bypass_document_validation,
                    )
                    break  # success for this sub-batch

                except (
                    pymongo_errors.AutoReconnect,
                    pymongo_errors.NetworkTimeout,
                    pymongo_errors.ExecutionTimeout,
                    pymongo_errors.CursorNotFound,
                ) as e:
                    # Transient connectivity/timeouts → backoff + shrink chunk by count
                    attempt += 1
                    if attempt > max_retries:
                        raise DatabaseWriteError(f"Error during bulk write: {e}") from e
                    sleep = (base_sleep * (2 ** (attempt - 1))) * (
                        1.0 + random.random() * 0.3
                    )
                    time.sleep(sleep)
                    chunk = max(min_chunk, chunk // 2)
                    continue

                except pymongo_errors.OperationFailure as e:
                    # Common transient server-side cancellation/timeouts/stepdowns
                    transient_codes = {50, 11600, 91, 189, 13436}
                    if getattr(e, "code", None) in transient_codes:
                        attempt += 1
                        if attempt > max_retries:
                            raise DatabaseWriteError(
                                f"Error during bulk write: {e}"
                            ) from e
                        sleep = (base_sleep * (2 ** (attempt - 1))) * (
                            1.0 + random.random() * 0.3
                        )
                        time.sleep(sleep)
                        chunk = max(min_chunk, chunk // 2)
                        continue
                    # Non-transient → fail fast
                    raise DatabaseWriteError(f"Error during bulk write: {e}") from e

                except pymongo_errors._OperationCancelled as e:  # type: ignore[attr-defined]
                    # Server explicitly cancelled the op → treat as transient.
                    attempt += 1
                    if attempt > max_retries:
                        raise DatabaseWriteError(f"Error during bulk write: {e}") from e
                    sleep = (base_sleep * (2 ** (attempt - 1))) * (
                        1.0 + random.random() * 0.3
                    )
                    time.sleep(sleep)
                    chunk = max(min_chunk, chunk // 2)
                    continue

                except Exception as e:
                    # Unknown errors: retry several times, then give up.
                    attempt += 1
                    if attempt > max_retries:
                        raise DatabaseWriteError(f"Error during bulk write: {e}") from e
                    sleep = (base_sleep * (2 ** (attempt - 1))) * (
                        1.0 + random.random() * 0.3
                    )
                    time.sleep(sleep)
                    chunk = max(min_chunk, chunk // 2)
                    continue

            # Move to next sub-slice inside this byte chunk
            start = end


# ---------------------------
# Hot-path helpers (unchanged logic)
# ---------------------------


def _get_num_add(list_of_edges: list[dict[str, str]]) -> int:
    """
    Count the number of unique addresses in the edge list [{from,to},...]
    """
    uniq: set[str] = set()
    for e in list_of_edges:
        f: str | None = e.get("from")
        t: str | None = e.get("to")
        if f is not None:
            uniq.add(f)
        if t is not None:
            uniq.add(t)
    return len(uniq)


def _get_vertices(edges: list[dict[str, str]]) -> list[str]:
    """
    Get the list of unique vertices.
    """
    uniq: set[str] = set()
    for e in edges:
        for v in e.values():
            uniq.add(v)
    return list(uniq)


def _preprocess_one_doc(doc: SubgraphDoc, max_vertices: int = 200) -> OutSubgraphDoc:
    """
    Preprocess a single subgraph document and return an OutSubgraphDoc:
      - Compute NumAddress (required)
      - Extract vertices
      - Rename 'address' -> 'X_address' (if X_address missing)
      - Optionally pass through: _id, chain, chainId, lastUpdatedAt
      - Filter out graphs with too many vertices
    """
    try:
        # Normalize edges to a list[dict[str, str]]
        raw_edges = doc.get("edges") or []
        if not isinstance(raw_edges, list):
            raw_edges = []
        edges: list[dict[str, str]] = []
        for e in raw_edges:
            f = e.get("from")
            t = e.get("to")
            if isinstance(f, str) and isinstance(t, str):
                edges.append({"from": f, "to": t})

        # Decide X_address
        x_address = doc.get("X_address")
        if not isinstance(x_address, str):
            x_address = cast(str | None, doc.get("address")) or ""

        # NumAddress & filter
        num_addr = _get_num_add(edges)
        if num_addr > max_vertices:
            raise ComputationError(
                f"Subgraph has too many vertices ({num_addr} > {max_vertices})."
            )

        # vertices
        vertices = _get_vertices(edges)

        # --- Build the typed output dict explicitly ---
        out: OutSubgraphDoc = {
            "NumAddress": num_addr,  # required in your TypedDict
            "edges": edges,  # optional but present now
            "vertices": vertices,  # optional but present now
        }

        # Optional passthrough fields if present and well-typed
        _id = doc.get("_id")
        out["_id"] = _id
        chain = doc.get("chain")
        if isinstance(chain, str):
            out["chain"] = chain
        chain_id = doc.get("chainId")
        out["chainId"] = chain_id
        lu = doc.get("lastUpdatedAt")
        if isinstance(lu, int):
            out["lastUpdatedAt"] = lu

        # Only set X_address if we actually have one
        if x_address:
            out["X_address"] = x_address

        return out

    except (SchemaMismatchError, ComputationError):
        raise
    except Exception as e:
        raise SchemaMismatchError(f"Error preprocessing document: {e}") from e


# ---------------------------
# Parallel-safe wrappers
# ---------------------------


def _safe_preprocess(doc: SubgraphDoc, max_vertices: int) -> Optional[OutSubgraphDoc]:
    """
    Worker function executed in a separate process.
    Returns the processed doc or None if it should be skipped.
    """
    try:
        return _preprocess_one_doc(doc, max_vertices=max_vertices)
    except (SchemaMismatchError, ComputationError):
        return None
    except Exception:
        return None


def _iter_in_batches(iterable: Iterable[SubgraphDoc], batch_size: int):
    """
    Yield lists of size up to batch_size from an iterable.
    """
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


# ---------------------------
# CSOT/Resume-safe cursor iteration
# ---------------------------


def _iter_with_resume(
    col: Collection[SubgraphDoc],
    projection: Dict[str, int],
    read_batch_docs: int,
    find_batch_size: int,
    logger,
    max_resume_retries: int = 10,
) -> Iterable[list[SubgraphDoc]]:
    """
    Iterate the whole collection in ascending _id order and yield logical batches.
    If the cursor is cancelled/timeout (_OperationCancelled or related), reopen
    the cursor with filter _id > last_id and continue, up to max_resume_retries.
    """
    last_id: Optional[Any] = None
    finished = False
    retries = 0

    while not finished:
        # Build filter to resume after last_id
        filt: Dict[str, Any] = {}
        if last_id is not None:
            filt = {"_id": {"$gt": last_id}}

        try:
            # no_cursor_timeout avoids server-side idle kill; CSOT is client-side.
            # max_time_ms=0 asks server not to time-limit the operation.
            # hint on _id helps the sorted scan be efficient.
            cursor = col.find(
                filt,
                projection,
                sort=[("_id", 1)],
                batch_size=find_batch_size,
                no_cursor_timeout=True,
                max_time_ms=0,
                hint=[("_id", 1)],
            )
        except Exception as e:
            # Fail fast on connection issues
            raise DatabaseConnectionError(f"Failed to open cursor: {e}") from e

        # Drain cursor into logical batches
        try:
            for logical in _iter_in_batches(cursor, read_batch_docs):
                # Update last_id from the last doc of this logical batch
                last_id = logical[-1].get("_id", last_id)
                yield logical

            # EOF without exceptions → finished
            finished = True

        except (pymongo_errors.OperationFailure,) as e:
            # OperationFailure can wrap "operation was interrupted" variants
            logger.warning(f"OperationFailure while scanning, will retry resume: {e}")
            retries += 1
            if retries > max_resume_retries:
                raise QueryError(
                    f"Exceeded max resume retries ({max_resume_retries})"
                ) from e
            time.sleep(min(2.0, 0.2 * retries))  # tiny backoff before resuming
            continue

        except (pymongo_errors.PyMongoError, pymongo_errors.CursorNotFound) as e:
            # Covers _OperationCancelled and network timeouts.
            logger.warning(f"PyMongoError while scanning, will retry resume: {e}")
            retries += 1
            if retries > max_resume_retries:
                raise QueryError(
                    f"Exceeded max resume retries ({max_resume_retries})"
                ) from e
            time.sleep(min(2.0, 0.2 * retries))
            continue

        except Exception as e:
            # Unknown error → try resume up to the limit
            logger.warning(f"Unknown scan error, will retry resume: {e}")
            retries += 1
            if retries > max_resume_retries:
                raise QueryError(
                    f"Exceeded max resume retries ({max_resume_retries})"
                ) from e
            time.sleep(min(2.0, 0.2 * retries))
            continue

        finally:
            try:
                cursor.close()
            except Exception:
                pass


# ---------------------------
# Main entry (parallelized + resume-safe)
# ---------------------------


def query_subgraph_to_mongo(
    chain: str,
    radius: int,
    out_collection_name: str,
    client: MongoDB,
    unique_key: str = "_id",
    max_vertices: int = 200,
    max_workers: int = 10,  # CPU-bound preprocessing
    read_batch_docs: int = 5000,  # logical docs per read batch
    bulk_batch_ops: int = 600,  # ↓ smaller default to reduce pressure per flush
) -> tuple[int, list[str]]:
    """
    Read raw subgraph from Mongo collection 'subgraph_{chain}_{radius}',
    preprocess in parallel (ProcessPool), directly upsert into the target collection
    `out_collection_name`, then return (number of upserted docs, list of written _id or X_address).

    Enhancements:
      - Resume-safe cursor iteration by ascending _id with _id > last_id on retry.
      - no_cursor_timeout=True + max_time_ms=0 to avoid server-side cancellations.
      - Resilient bulk_write with byte-aware chunking and transient retries.
    """
    logger = get_logger(f"query subgraph of {chain}")
    logger.info(f"Querying subgraph for chain={chain}, radius={radius}")

    # Server-side fetch size for the cursor
    FIND_BATCH_SIZE = 1000
    BYPASS_VALIDATION = True  # set False if you rely on collection validators

    PROJECTION = {
        "_id": 1,
        "edges": 1,
        "address": 1,
        "X_address": 1,
        "chain": 1,
        "chainId": 1,
        "lastUpdatedAt": 1,
    }

    # Connect to Mongo
    try:
        db: Database[Any] = client.db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise DatabaseConnectionError(f"Failed to connect to MongoDB: {e}") from e

    try:
        src_col: Collection[SubgraphDoc] = db[f"subgraph_{chain}_{radius}"]
        dst_col: Collection[OutSubgraphDoc] = db[out_collection_name]

        # Create indexes (safe if already exist)
        try:
            _ = dst_col.create_index(
                [(unique_key, 1)], name=f"{unique_key}_idx", unique=False
            )
            _ = dst_col.create_index([("X_address", 1)], name="xaddr_idx", unique=False)
            _ = dst_col.create_index(
                [("NumAddress", 1)], name="numaddr_idx", unique=False
            )
        except Exception as e:
            # Non-fatal if index already exists or creation races
            logger.debug(f"Index creation skipped/failed (non-fatal): {e}")

        ops: list[UpdateOne] = []
        written_ids: list[str] = []

        # Parallel processing loop with resume-safe iterator
        with ProcessPoolExecutor(max_workers=max_workers) as exe:
            for raw_batch in _iter_with_resume(
                src_col,
                PROJECTION,
                read_batch_docs=read_batch_docs,
                find_batch_size=FIND_BATCH_SIZE,
                logger=logger,
                max_resume_retries=10,
            ):
                # Submit this batch to worker processes
                futures = [
                    exe.submit(_safe_preprocess, raw, max_vertices) for raw in raw_batch
                ]

                # Collect results as they complete
                for fut in as_completed(futures):
                    proc = fut.result()
                    if not proc:
                        continue

                    # Upsert key
                    if unique_key in proc:
                        flt: Dict[str, Any] = {unique_key: proc[unique_key]}
                    elif unique_key == "X_address" and "X_address" in proc:
                        flt = {"X_address": proc["X_address"]}
                    else:
                        # No usable key -> skip
                        continue

                    ops.append(UpdateOne(flt, {"$set": proc}, upsert=True))
                    written_ids.append(proc.get("_id", proc.get("X_address") or ""))

                    # Flush when enough ops accumulated (by count). The helper will also split by bytes.
                    if len(ops) >= bulk_batch_ops:
                        try:
                            _resilient_bulk_write(
                                dst_col,
                                ops,
                                ordered=False,
                                bypass_document_validation=BYPASS_VALIDATION,
                                max_retries=8,
                                initial_chunk=400,
                                min_chunk=25,
                            )
                        except Exception as e:
                            raise DatabaseWriteError(
                                f"Error during bulk write: {e}"
                            ) from e
                        finally:
                            ops.clear()

        # Final flush
        if ops:
            try:
                _resilient_bulk_write(
                    dst_col,
                    ops,
                    ordered=False,
                    bypass_document_validation=BYPASS_VALIDATION,
                    max_retries=8,
                    initial_chunk=400,
                    min_chunk=25,
                )
            except Exception as e:
                raise DatabaseWriteError(f"Error during final bulk write: {e}") from e

    except (DatabaseWriteError, DatabaseConnectionError):
        # Preserve your error taxonomy
        raise
    except Exception as e:
        # Surface as QueryError to keep taxonomy consistent
        raise QueryError(f"Query or processing error: {e}") from e

    logger.info(f"Upserted {len(written_ids)} docs into '{out_collection_name}'")
    return len(written_ids), written_ids


def _first_last_ids(col: Collection, logger) -> tuple[ObjectId | None, ObjectId | None]:
    """
    Fetch the smallest and largest _id (assuming ObjectId) to define the scan range.
    Returns (min_id, max_id) or (None, None) if collection is empty.
    """
    try:
        first = col.find({}, {"_id": 1}).sort([("_id", 1)]).limit(1)
        last = col.find({}, {"_id": 1}).sort([("_id", -1)]).limit(1)
        min_id = next(first, {}).get("_id")
        max_id = next(last, {}).get("_id")
        return (min_id, max_id)
    except Exception as e:
        logger.warning(f"Failed to read first/last _id: {e}")
        return (None, None)


def _split_objectid_range(
    min_id: ObjectId, max_id: ObjectId, parts: int
) -> list[tuple[ObjectId, ObjectId | None]]:
    """
    Split the ObjectId timestamp range into `parts` contiguous half-open intervals.
    Range i is [start_i, end_i), the last interval has end=None (open).
    """
    # ObjectId’s first 4 bytes are seconds since epoch
    t0 = min_id.generation_time.timestamp()
    t1 = max_id.generation_time.timestamp()
    if t1 <= t0 or parts <= 1:
        return [(min_id, None)]  # single slice

    dt = (t1 - t0) / parts
    out = []
    for i in range(parts):
        start_ts = int(t0 + i * dt)
        end_ts = int(t0 + (i + 1) * dt)
        start_id = ObjectId.from_datetime(datetime.utcfromtimestamp(start_ts))
        if i == parts - 1:
            out.append((start_id, None))
        else:
            end_id = ObjectId.from_datetime(datetime.utcfromtimestamp(end_ts))
            out.append((start_id, end_id))
    # Ensure the very first slice starts at true min_id
    if out:
        out[0] = (min_id, out[0][1])
    return out


def _split_generic_sorted_ranges(
    col: Collection,
    parts: int,
    logger,
) -> list[tuple[Any, Any | None]]:
    """
    Build `parts` half-open ranges over _id using server-side boundaries.
    Tries $bucketAuto first (efficient & balanced). Falls back to probing split keys.
    Works for any BSON-orderable _id type (str, int, date, ObjectId, etc.).
    """
    parts = max(1, parts)

    # 1) Try $bucketAuto to let Mongo choose balanced buckets by _id
    try:
        pipeline = [{"$bucketAuto": {"groupBy": "$_id", "buckets": parts}}]
        buckets = list(col.aggregate(pipeline, allowDiskUse=True))
        # Each bucket has "min" and "max"
        ranges: list[tuple[Any, Any | None]] = []
        for i, b in enumerate(buckets):
            start = b.get("min")
            end = b.get("max")
            # Last bucket is open-ended on the right
            if i == len(buckets) - 1:
                ranges.append((start, None))
            else:
                ranges.append((start, end))
        # Ensure first bucket starts at the true min _id
        try:
            first_doc = next(col.find({}, {"_id": 1}).sort([("_id", 1)]).limit(1), None)
            if first_doc and ranges:
                ranges[0] = (first_doc["_id"], ranges[0][1])
        except Exception:
            pass
        if ranges:
            return ranges
    except Exception as e:
        logger.debug(f"$bucketAuto failed, will fallback to probing split keys: {e}")

    # 2) Fallback: probe approximate split keys via sorted skips
    try:
        total = col.estimated_document_count()
        if total == 0:
            return []
        # Compute split positions (quantiles by count)
        split_keys: list[Any] = []
        for i in range(parts):
            # position at i/parts
            pos = int((total * i) / parts)
            # mongo skip can be heavy, but acceptable for modest 'parts'
            cursor = col.find({}, {"_id": 1}).sort([("_id", 1)]).skip(pos).limit(1)
            doc = next(cursor, None)
            if doc:
                split_keys.append(doc["_id"])

        # Build half-open ranges from split_keys
        ranges: list[tuple[Any, Any | None]] = []
        for i in range(len(split_keys)):
            lo = split_keys[i]
            hi = split_keys[i + 1] if i + 1 < len(split_keys) else None
            ranges.append((lo, hi))

        # Ensure we have at least one
        return ranges or []  # min_id will be substituted by caller if needed
    except Exception as e:
        logger.warning(f"Fallback probing for split keys failed: {e}")
        # Last resort: single range (no parallelism)
        first = next(col.find({}, {"_id": 1}).sort([("_id", 1)]).limit(1), None)
        return [(first["_id"], None)] if first else []


def _iter_with_resume_range(
    col: Collection,
    projection: dict[str, int],
    read_batch_docs: int,
    find_batch_size: int,
    logger,
    low_id: ObjectId | None,
    high_id: ObjectId | None,  # half-open: _id < high_id
    max_resume_retries: int = 10,
):
    """
    Like _iter_with_resume, but constrained within an _id window: low_id <= _id < high_id (if provided).
    """
    last_id: Any | None = None
    finished = False
    retries = 0

    while not finished:
        filt: dict[str, Any] = {}
        # range predicates
        if low_id is not None and high_id is not None:
            filt["_id"] = {"$gte": low_id, "$lt": high_id}
        elif low_id is not None:
            filt["_id"] = {"$gte": low_id}
        elif high_id is not None:
            filt["_id"] = {"$lt": high_id}

        # resume predicate
        if last_id is not None:
            # refine lower bound to resume from last_id
            if "_id" in filt:
                cur = filt["_id"]
                if "$gte" in cur:
                    cur["$gt"] = last_id  # stricter resume
                    cur.pop("$gte", None)
                elif "$lt" in cur:
                    # have only upper bound; add resume lower bound
                    cur["$gt"] = last_id
                else:
                    cur["$gt"] = last_id
            else:
                filt["_id"] = {"$gt": last_id}

        try:
            cursor = col.find(
                filt,
                projection,
                sort=[("_id", 1)],
                batch_size=find_batch_size,
                no_cursor_timeout=True,
                max_time_ms=0,
                hint=[("_id", 1)],
            )
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to open ranged cursor: {e}") from e

        try:
            for logical in _iter_in_batches(cursor, read_batch_docs):
                last_id = logical[-1].get("_id", last_id)
                yield logical
            finished = True

        except (pymongo_errors.OperationFailure,) as e:
            logger.warning(f"OperationFailure in range scan, retry resume: {e}")
            retries += 1
            if retries > max_resume_retries:
                raise QueryError(
                    f"Exceeded max resume retries ({max_resume_retries})"
                ) from e
            time.sleep(min(2.0, 0.2 * retries))
            continue

        except (pymongo_errors.PyMongoError, pymongo_errors.CursorNotFound) as e:
            logger.warning(f"PyMongoError in range scan, retry resume: {e}")
            retries += 1
            if retries > max_resume_retries:
                raise QueryError(
                    f"Exceeded max resume retries ({max_resume_retries})"
                ) from e
            time.sleep(min(2.0, 0.2 * retries))
            continue

        except Exception as e:
            logger.warning(f"Unknown range scan error, retry resume: {e}")
            retries += 1
            if retries > max_resume_retries:
                raise QueryError(
                    f"Exceeded max resume retries ({max_resume_retries})"
                ) from e
            time.sleep(min(2.0, 0.2 * retries))
            continue

        finally:
            try:
                cursor.close()
            except Exception:
                pass


def query_subgraph_from_mongo_parallel(
    out_collection_name: str,
    client: MongoDB,
    total_limit: Optional[int] = None,  # None => read all
    read_batch_docs: int = 5000,
    find_batch_size: int = 1000,
    max_resume_retries: int = 10,
    parallelism: int = 4,  # number of concurrent range readers
    global_sort: bool = False,  # set True to sort final results by _id
) -> tuple[int, list[str]]:
    """
    Parallel reader that splits the _id space into `parallelism` slices and scans them concurrently.
    Guarantees in-slice ordering; global ordering can be enforced with `global_sort=True`.
    Honors `total_limit` across all workers via a shared counter.
    """
    logger = get_logger(f"read subgraph (parallel) from '{out_collection_name}'")
    logger.info(
        f"Reading (parallel={parallelism}) from '{out_collection_name}', total_limit={total_limit}"
    )

    db: Database[Any] = client.db
    col: Collection[OutSubgraphDoc] = db[out_collection_name]

    proj = {
        "_id": 1,
        "NumAddress": 1,
        "vertices": 1,
        "edges": 1,
        "X_address": 1,
        "chain": 1,
        "chainId": 1,
        "lastUpdatedAt": 1,
    }

    # Find min/max _id to define the range
    min_id, max_id = _first_last_ids(col, logger)
    if min_id is None or max_id is None:
        logger.info("Collection is empty. Return (0, []).")
        return 0, []

    parts = max(1, parallelism)

    # Choose a slicing strategy based on _id type
    from bson.objectid import ObjectId as _OID  # local alias to avoid shadowing

    if isinstance(min_id, _OID) and isinstance(max_id, _OID):
        slices = _split_objectid_range(min_id, max_id, parts)
    else:
        # Generic splitter that works for str/int/UUID/etc. using bucketAuto or fallback
        slices = _split_generic_sorted_ranges(col, parts, logger)

    # Safety: always have at least one slice
    if not slices:
        slices = [(min_id, None)]

    results: list[OutSubgraphDoc] = []
    lock = Lock()
    remaining = [total_limit if total_limit is not None else float("inf")]

    def worker(
        low_id: ObjectId | None, high_id: ObjectId | None
    ) -> list[OutSubgraphDoc]:
        local: list[OutSubgraphDoc] = []
        for logical in _iter_with_resume_range(
            col,
            proj,
            read_batch_docs=read_batch_docs,
            find_batch_size=find_batch_size,
            logger=logger,
            low_id=low_id,
            high_id=high_id,
            max_resume_retries=max_resume_retries,
        ):
            with lock:
                if remaining[0] <= 0:
                    break
                # take up to remaining items from this logical batch
                take = (
                    len(logical)
                    if remaining[0] == float("inf")
                    else int(min(len(logical), remaining[0]))
                )
                remaining[0] = remaining[0] - (
                    0 if remaining[0] == float("inf") else take
                )
            if take > 0:
                local.extend(logical[:take])
            if remaining[0] == 0:
                break
        return local

    # Threaded I/O parallelism (PyMongo is I/O bound)
    with ThreadPoolExecutor(max_workers=len(slices)) as tp:
        futs = [tp.submit(worker, lo, hi) for (lo, hi) in slices]
        for f in as_completed(futs):
            chunk = f.result()
            if chunk:
                results.extend(chunk)

    if global_sort:
        results.sort(key=lambda d: d.get("_id"))

    # Convert documents to written_ids format
    written_ids = [str(doc.get("_id", doc.get("X_address") or "")) for doc in results]

    logger.info(
        f"Parallel read returned {len(written_ids)} documents from '{out_collection_name}'"
    )
    return len(written_ids), written_ids
