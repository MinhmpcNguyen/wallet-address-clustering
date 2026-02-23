from __future__ import annotations

import asyncio
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

TZ = ZoneInfo("Asia/Bangkok")
API_BASE = os.getenv("API_BASE", "http://api:8000")


def _build_time_window(now: datetime) -> tuple[int, int]:
    """Return (start_time, end_time) as UNIX seconds:
    start_time = now - 14 days, end_time = now - 1 day (both in ICT)."""
    start_dt = now - timedelta(days=14)
    end_dt = now - timedelta(days=1)
    return int(start_dt.timestamp()), int(end_dt.timestamp())


def build_run_all_payload(now: datetime) -> dict[str, Any]:
    """Build the payload with dynamic time window in `common`."""
    start_ts, end_ts = _build_time_window(now)

    return {
        "order": [
            "graph_exporter",
            "graph_prune",
            "exchange_deposit_wallets",
            "deposits_and_users",
            "subgraph_exporter",
        ],
        "common": {
            "chain": "ethereum",
            "start_time": start_ts,
            "end_time": end_ts,
            "delay": 0,
            "run_now": True,
            "max_workers": 4,
        },
        "graph_exporter": {
            "period": 900,
            "source": [],  # empty by default
            "last_synced_file": ".data/last_synced_graph_exporter.txt",
            "interval": 86400,
        },
        "graph_prune": {
            "timespan": 120,
            "batch_size_query": 100_000,
            "batch_size_thread": 100,
            "last_synced_file": ".data/0x1_graph_prune.txt",
            "interval": 86400,
        },
        "exchange_deposit_wallets": {
            "period": 3600,
            "source": [],
            "last_synced_file": ".data/last_synced_deposit_wallets.txt",
            "interval": 86400,
        },
        "deposits_and_users": {
            "batch_size": 1000,
            "source": [],
            "interval": 3600,
            "start_time": 1755799200,
            "last_synced_file": ".data/last_synced_deposits_users.txt",
        },
        "subgraph_exporter": {"radius": 2, "batch_size": 100},
    }


# Keep your canonical training payload as-is
RUN_ALL_AND_TRAIN_PAYLOAD = {
    "pipeline": {
        "time_amount": {
            "chain": "ethereum",
            "batch_size": 100,
            "max_workers": 4,
            "radius": 2,
        },
        "deposit_reuse_pairs": {
            "chain": "ethereum",
            "batch_size": 100,
            "max_workers": 4,
            "pairs_collection_name": "deposit_reuse_pairs_ethereum",
        },
        "node_embedding": {
            "chain": "ethereum",
            "out_collection_name": "subgraph_ethereum_2_preprocessed",
            "dest_collection_name": "node_embeddings_ethereum_2",
            "radius": 2,
        },
        "combine_features": {
            "chain": "ethereum",
            "from_col_name": "time_amount_features_from",
            "to_col_name": "time_amount_features_to",
            "embedding_col_name": "node_embeddings_ethereum_2",
            "pairs_col_name": "deposit_reuse_pairs_ethereum",
            "out_train_col_name": "train_data_ethereum_2",
            "out_test_col_name": "test_data_ethereum_2",
            "compute_embedding_similarity": False,
            "train_ratio": 0.9,
            "balance_train_by_label": True,
        },
        "background": False,
    },
    "training": {
        "train_collection": "train_data_ethereum_2",
        "test_collection": "test_data_ethereum_2",
        "drop_cols": ["Unnamed: 0", "Diff2_Vec_Simi"],
        "smote_k": 5,
        "num_leaves": 190,
        "feature_fraction": 0.4,
        "max_depth": 40,
        "output_dir": "output",
        "model_txt_name": "lightgbm_model.txt",
        "train_csv_name": "train_data.csv",
        "test_csv_name": "test_data.csv",
        "hf_repo_basename": "string",
        "hf_private": False,
    },
    "background": False,
}


async def call(endpoint: str, payload: dict[str, Any], timeout: float | None):
    url = f"{API_BASE}{endpoint}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, json=payload)
        _ = r.raise_for_status()
        print(f"[{datetime.now(TZ)}] {endpoint} OK:", r.json())


DATA_DIR = Path("/app/.data")  # this matches the volume you mount in docker-compose


def _clear_data_dir():
    """Delete all files in /app/.data before each run."""
    if DATA_DIR.exists() and DATA_DIR.is_dir():
        for item in DATA_DIR.iterdir():
            if item.is_file() or item.is_symlink():
                item.unlink(missing_ok=True)
            elif item.is_dir():
                shutil.rmtree(item, ignore_errors=True)


async def run_once_monthly():
    # Clear .data before starting jobs
    _clear_data_dir()

    now = datetime.now(TZ)
    run_all_payload = build_run_all_payload(now)
    await call("/all/all-graph/run", run_all_payload, timeout=600)
    await call(
        "/all/data-collection-and-train/run", RUN_ALL_AND_TRAIN_PAYLOAD, timeout=None
    )


async def _sleep_until_next_month_0130_ict():
    """Sleep until 01:30 AM on the 1st of the next month (ICT)."""
    now = datetime.now(TZ)
    month, year = now.month, now.year
    if month == 12:
        target = datetime(year + 1, 1, 1, 1, 30, tzinfo=TZ)
    else:
        target = datetime(year, month + 1, 1, 1, 30, tzinfo=TZ)
    if now >= target:
        if target.month == 12:
            target = datetime(target.year + 1, 1, 1, 1, 30, tzinfo=TZ)
        else:
            target = datetime(target.year, target.month + 1, 1, 1, 30, tzinfo=TZ)
    await asyncio.sleep((target - now).total_seconds())


async def main_loop():
    while True:
        await _sleep_until_next_month_0130_ict()
        try:
            await run_once_monthly()
        except Exception as e:
            print(f"[{datetime.now(TZ)}] Scheduler error: {e}")


if __name__ == "__main__":
    asyncio.run(main_loop())
