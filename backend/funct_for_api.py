from __future__ import annotations

import os
import time
from functools import partial
from typing import Any, Callable, TypeVar

import anyio
import pandas as pd  # pyright: ignore [reportMissingTypeStubs]
from hooks.errors import (
    AppError,
    DataParsingError,
    EmptyDatasetError,
    FileSystemError,
    SerializationError,
)
from pymongo.database import Database
from utils.logger_utils import get_logger

logger = get_logger("AsyncOps")

T = TypeVar("T")


async def to_thread(func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    if kwargs:
        # wrap func+kwargs so run_sync only sees positional args
        return await anyio.to_thread.run_sync(partial(func, *args, **kwargs))  # pyright: ignore [reportUnknownVariableType,reportUnknownMemberType,reportAttributeAccessIssue]
    return await anyio.to_thread.run_sync(func, *args)  # pyright: ignore [reportUnknownVariableType,reportUnknownMemberType,reportAttributeAccessIssue]


# ---------- async export helper (used by training) ----------
async def export_collection_to_csv_async(
    db: Database[Any], col_name: str, out_csv: str
) -> int:
    t0 = time.time()
    try:
        # Cursor (blocking)
        cur = await to_thread(lambda: db[col_name].find({}))

        # Materialize → DataFrame (blocking)
        df = await to_thread(lambda: pd.DataFrame(list(cur)))

        if df.empty:
            raise EmptyDatasetError(f"Collection '{col_name}' is empty")

        if "_id" in df.columns:
            df.drop(columns=["_id"], inplace=True)

        # FS ops (blocking)
        await to_thread(os.makedirs, os.path.dirname(out_csv), exist_ok=True)
        _ = await to_thread(df.to_csv, out_csv, index=False)  # pyright: ignore [reportUnknownMemberType,reportUnknownArgumentType]

        logger.info(
            "Exported %d rows from '%s' to '%s' in %.2fs",
            len(df),
            col_name,
            out_csv,
            time.time() - t0,
        )
        return len(df)

    except (EmptyDatasetError, FileSystemError, DataParsingError, SerializationError):
        raise
    except Exception as exc:
        # Map broadly into domain errors with context
        from hooks.errors import QueryError  # local import to avoid circulars

        msg = str(exc)
        if "find" in msg or "pymongo" in msg:
            raise (
                QueryError("Mongo query failed")
                .with_context(collection=col_name)
                .with_cause(exc)
            )
        if isinstance(exc, (OSError, IOError)):
            raise (
                FileSystemError("Filesystem error during export")
                .with_context(path=out_csv)
                .with_cause(exc)
            )
        try:
            # If DataFrame construction failed
            if "DataFrame" in msg or "ndarray" in msg:
                raise (
                    DataParsingError("Failed to convert cursor to DataFrame")
                    .with_context(collection=col_name)
                    .with_cause(exc)
                )
        except AppError as e2:
            raise e2
        raise AppError.from_exc(
            exc, message="Unexpected error during export"
        ).with_context(collection=col_name, path=out_csv)
