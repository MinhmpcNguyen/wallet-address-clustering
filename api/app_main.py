# api/app_main.py
from __future__ import annotations

import os
import time
from typing import Any, Literal, TypeVar

from backend.funct_for_api import (
    export_collection_to_csv_async,
    to_thread,
)
from cli.deposit_reuse.deposits_and_users_collect import DepositsUsersScheduler
from cli.deposit_reuse.exchange_deposit_wallets import ExchangeWallets
from cli.wallet_graph.graph_exporter import GraphLoaderSchedulerJob
from clients import Clients
from constants.network_constants import Chains
from fastapi import FastAPI, HTTPException
from hooks.errors import (
    AppError,
    DataValidationError,
    ExternalServiceError,
    SerializationError,
    TrainingError,
)
from jobs.subgraph_exporter_job_v2 import SubgraphExporterJob
from jobs.wallet_graph.graph_prune_job import GraphPruneJob
from model_lightgbm.initiate_model import LightGBMTrainer
from pydantic import BaseModel
from schemas.get_graph_schema import (
    CommonParams,
    DepositsAndUsersPayload,
    ExchangeDepositWalletsPayload,
    GraphExporterPayload,
    GraphPrunePayload,
    RunResult,
    RunStatus,
    SubgraphExporterPayload,
)
from schemas.graph_schema import GraphExporterJobKwargs
from schemas.pipeline_schema import (
    CombineFeaturesReq,
    DepositReusePairReq,
    HealthResp,
    PipelineStepResp,
    QuerySubgraphReq,
    RunAllAndTrainReq,
    RunAllAndTrainResp,
    RunAllReq,
    RunAllResp,
    TimeAmountReq,
    TrainFromMongoReq,
    TrainFromMongoResp,
)
from service.combine_features_service import ProcessTrainingDatasetMongo
from service.deposit_reuse_pairs_job import DepositReusePairJob
from service.huggingface_upload import HuggingFaceUploader
from service.query_subgraph import (
    query_subgraph_to_mongo,
)
from service.time_amount_exporter_service import TimeAmountExporterJob
from utils.logger_utils import get_logger

from api.config import Configs
from api.lifecycle import lifespan
from api.override import (
    DepositsAndUsersOverrides,
    ExchangeDepositWalletsOverrides,
    GraphExporterOverrides,
    GraphPruneOverrides,
    SubgraphExporterOverrides,
)

app = FastAPI(lifespan=lifespan)

logger = get_logger("MainAPI")
clickhouse = Clients.get_clickhouse_client().get_clickhouse_service()
arangodb = Clients.get_arango_client()
mongo_entity = Clients.get_mongo_entity_client().get_mongodb_entity_service()
mongodb = Clients.get_mongo_client().get_mongodb_service()


def _raise_http(e: AppError) -> None:
    """Convert AppError to HTTPException with structured detail."""
    raise HTTPException(status_code=e.http_status, detail=e.to_dict())


# ---------- health ----------
@app.get("/health", response_model=HealthResp)
async def health() -> HealthResp:
    ts = int(time.time())
    logger.debug("/health ping at ts=%d", ts)
    return HealthResp(ok=True, ts=ts)


@app.post(
    "/graph/exporter/run", response_model=RunResult, tags=[Configs.get_graph_tag()]
)
async def run_graph_exporter(payload: GraphExporterPayload):
    """
    Run the Graph Loader Scheduler for the Graph Exporter job with the given payload.

    **Request Parameters (GraphExporterPayload):**
    - chain (str): Blockchain name (must exist in `Chains.mapping`). Example: "ethereum".
    - start_time (int): Start timestamp (UNIX epoch, seconds).
    - end_time (int): End timestamp (UNIX epoch, seconds).
    - delay (int, optional): Delay before scheduler starts (seconds). Default = 0.
    - run_now (bool, optional): Whether to run immediately. Default = True.
    - period (int, optional): Batch window size in seconds. Default = 900 (15 minutes).
    - source (list[str] | None, optional): Data sources to export (e.g., ["transactions", "token_transfers"]). Default = [].
    - last_synced_file (str, optional): Path to last synced file. Default = ".data/last_synced_graph_exporter.txt".
    - interval (int, optional): Scheduler interval in seconds. Default = 86400 (1 day).
    - max_workers (int, optional): Number of worker threads. Default = 4.

    **Returns (RunResult):**
    - status (str): Execution status ("completed" if successful).
    - chain (str): Blockchain name.
    - chain_id (int): Internal chain identifier.
    - last_synced_file (str): Path of the last synced state file.
    - start_time (int): Start timestamp used for the job.
    - end_time (int): End timestamp used for the job.
    - interval (int): Scheduler interval (seconds).
    - delay (int): Scheduler delay (seconds).
    - run_now (bool): Whether the job ran immediately.
    - period (int): Batch window size in seconds.
    - max_workers (int): Number of worker threads.
    - sources (list[str]): Data sources included in the run.

    **Raises:**
    - HTTPException 400: If the given chain is not supported.
    - HTTPException 500: If the graph exporter job fails during execution.
    """
    chain = payload.chain.lower()
    if chain not in Chains.mapping:
        raise HTTPException(status_code=400, detail=f"Chain '{chain}' is not supported")

    chain_id = Chains.mapping[chain]

    sources: list[str] = payload.source if payload.source else []

    graph_exporter_job_kwargs: GraphExporterJobKwargs = {
        "chain_id": chain_id,
        "sources": sources,
        "mongo_klg": mongo_entity,
        "cassandra": clickhouse,
        "batch_size": payload.period,
        "max_workers": payload.max_workers,
        "hot_wallets": set(),
        "burn_wallets": set(),
    }

    job_graph_loader_scheduler = GraphLoaderSchedulerJob(
        chain_id=chain_id,
        interval=payload.interval,
        delay=payload.delay,
        run_now=payload.run_now,
        last_synced_file=payload.last_synced_file,
        start_timestamp=payload.start_time,
        end_timestamp=payload.end_time,
        graph_exporter_job_kwargs=graph_exporter_job_kwargs,
    )

    try:
        await to_thread(job_graph_loader_scheduler.run)  # pyright: ignore [reportUnknownArgumentType,reportUnknownMemberType]
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Graph exporter failed: {e}"
        ) from e

    return RunResult(
        status=RunStatus.COMPLETED,
        chain=chain,
        chain_id=chain_id,
        last_synced_file=payload.last_synced_file,
        start_time=payload.start_time,
        end_time=payload.end_time,
        interval=payload.interval,
        delay=payload.delay,
        run_now=payload.run_now,
        period=payload.period,
        max_workers=payload.max_workers,
        sources=sources,
    )


@app.post("/graph/prune/run", response_model=RunResult, tags=[Configs.get_graph_tag()])
async def run_graph_prune(payload: GraphPrunePayload):
    """
    Run the Graph Prune job with the given payload.

    **Request Parameters (GraphPrunePayload):**
    - chain (str): Blockchain name (must exist in `Chains.mapping`). Example: "ethereum".
    - start_time (int): Start timestamp (UNIX epoch, seconds).
    - end_time (int): End timestamp (UNIX epoch, seconds).
    - delay (int, optional): Delay before scheduler starts (seconds). Default = 0.
    - run_now (bool, optional): Whether to run immediately. Default = True.
    - timespan (int, optional): Time span in seconds for pruning. Default = 120.
    - batch_size_query (int, optional): Batch size for database queries. Default = 100000.
    - batch_size_thread (int, optional): Batch size for worker threads. Default = 100.
    - last_synced_file (str, optional): Path to last synced file. Default = ".data/0x1_graph_prune.txt".
    - interval (int, optional): Scheduler interval in seconds. Default = 86400 (1 day).
    - max_workers (int, optional): Number of worker threads. Default = 4.

    **Returns (RunResult):**
    - status (str): Execution status ("completed" if successful).
    - chain (str): Blockchain name.
    - chain_id (int): Internal chain identifier.
    - last_synced_file (str): Path of the last synced state file.
    - start_time (int): Start timestamp used for the job.
    - end_time (int): End timestamp used for the job.
    - interval (int): Scheduler interval (seconds).
    - delay (int): Scheduler delay (seconds).
    - run_now (bool): Whether the job ran immediately.
    - timespan (int): Time span in seconds for pruning.
    - batch_size_query (int): Batch size for database queries.
    - batch_size_thread (int): Batch size for worker threads.
    - max_workers (int): Number of worker threads.

    **Raises:**
    - HTTPException 400: If the given chain is not supported.
    - HTTPException 500: If the graph prune job fails during execution.
    """
    chain = payload.chain.lower()
    if chain not in Chains.mapping:
        raise HTTPException(status_code=400, detail=f"Chain '{chain}' is not supported")

    chain_id = Chains.mapping[chain]

    job = GraphPruneJob(
        chain_id=chain_id,
        batch_size_query=payload.batch_size_query,
        batch_size_thread=payload.batch_size_thread,
        max_workers=payload.max_workers,
        timespan=payload.timespan,
        start_timestamp=payload.start_time,
        end_timestamp=payload.end_time,
        last_synced_file=payload.last_synced_file,
        interval=payload.interval,
        delay=payload.delay,
        run_now=payload.run_now,
    )

    try:
        await to_thread(job.run)  # pyright: ignore [reportUnknownArgumentType,reportUnknownMemberType]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Graph prune failed: {e}") from e

    return RunResult(
        status=RunStatus.COMPLETED,
        chain=chain,
        chain_id=chain_id,
        last_synced_file=payload.last_synced_file,
        start_time=payload.start_time,
        end_time=payload.end_time,
        interval=payload.interval,
        delay=payload.delay,
        run_now=payload.run_now,
        timespan=payload.timespan,
        batch_size_query=payload.batch_size_query,
        batch_size_thread=payload.batch_size_thread,
        max_workers=payload.max_workers,
    )


@app.post(
    "/graph/exchange-deposit-wallets/run",
    response_model=RunResult,
    tags=[Configs.get_graph_tag()],
)
async def run_exchange_deposit_wallets(payload: ExchangeDepositWalletsPayload):
    """
    Run the Exchange Deposit Wallets job with the given payload.

    **Request Parameters (ExchangeDepositWalletsPayload):**
    - chain (str): Blockchain name (must exist in `Chains.mapping`). Example: "ethereum".
    - start_time (int): Start timestamp (UNIX epoch, seconds).
    - end_time (int): End timestamp (UNIX epoch, seconds).
    - delay (int, optional): Delay before scheduler starts (seconds). Default = 0.
    - run_now (bool, optional): Whether to run immediately. Default = True.
    - period (int, optional): Batch window size in seconds for the job. Default = 900 (15 minutes).
    - source (list[str] | None, optional): Data sources to include (e.g., ["transactions", "token_transfers"]). Default = [].
    - last_synced_file (str, optional): Path to last synced file. Default = ".data/last_synced_exchange_wallets.txt".
    - interval (int, optional): Scheduler interval in seconds. Default = 86400 (1 day).
    - max_workers (int, optional): Number of worker threads. Default = 4.

    **Returns (RunResult):**
    - status (str): Execution status ("completed" if successful).
    - chain (str): Blockchain name.
    - chain_id (int): Internal chain identifier.
    - last_synced_file (str): Path of the last synced state file.
    - start_time (int): Start timestamp used for the job.
    - end_time (int): End timestamp used for the job.
    - interval (int): Scheduler interval (seconds).
    - delay (int): Scheduler delay (seconds).
    - run_now (bool): Whether the job ran immediately.
    - period (int): Batch window size in seconds.
    - max_workers (int): Number of worker threads.
    - sources (list[str] | None): Data sources included in the run.

    **Raises:**
    - HTTPException 400: If the given chain is not supported.
    - HTTPException 500: If the exchange deposit wallets job fails during execution.
    """
    chain = payload.chain.lower()
    if chain not in Chains.mapping:
        raise HTTPException(status_code=400, detail=f"Chain '{chain}' is not supported")

    chain_id = Chains.mapping[chain]

    job = ExchangeWallets(
        cassandra=clickhouse,
        mongodb=mongodb,
        chain_id=chain_id,
        start_timestamp=payload.start_time,
        end_timestamp=payload.end_time,
        period=payload.period,
        interval=payload.interval,
        delay=payload.delay,
        run_now=payload.run_now,
        max_workers=payload.max_workers,
        last_synced_file=payload.last_synced_file,
        sources=list(payload.source) if payload.source else None,
    )

    try:
        await to_thread(job.run)  # pyright: ignore [reportUnknownArgumentType,reportUnknownMemberType]
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Exchange deposit wallets failed: {e}"
        ) from e

    return RunResult(
        status=RunStatus.COMPLETED,
        chain=chain,
        chain_id=chain_id,
        last_synced_file=payload.last_synced_file,
        start_time=payload.start_time,
        end_time=payload.end_time,
        interval=payload.interval,
        delay=payload.delay,
        run_now=payload.run_now,
        period=payload.period,
        max_workers=payload.max_workers,
        sources=list(payload.source) if payload.source else None,
    )


@app.post(
    "/graph/deposits-and-users/run",
    response_model=RunResult,
    tags=[Configs.get_graph_tag()],
)
async def run_deposits_and_users(payload: DepositsAndUsersPayload):
    """
    Run the Deposits & Users scheduler with the given payload.

    **Request Parameters (DepositsAndUsersPayload):**
    - chain (str): Blockchain name (must exist in `Chains.mapping`). Example: "ethereum".
    - start_time (int): Start timestamp (UNIX epoch, seconds).
    - end_time (int): End timestamp (UNIX epoch, seconds).
    - delay (int, optional): Delay before scheduler starts (seconds). Default = 0.
    - run_now (bool, optional): Whether to run immediately. Default = True.
    - batch_size (int, optional): Number of records to process per batch. Default = 1000.
    - source (list[str] | None, optional): Data sources to include (e.g., ["transactions", "deposits"]). Default = [].
    - last_synced_file (str, optional): Path to last synced file. Default = ".data/last_synced_deposits_and_users.txt".
    - interval (int, optional): Scheduler interval in seconds. Default = 86400 (1 day).
    - max_workers (int, optional): Number of worker threads. Default = 4.

    **Returns (RunResult):**
    - status (str): Execution status ("completed" if successful).
    - chain (str): Blockchain name.
    - chain_id (int): Internal chain identifier.
    - last_synced_file (str): Path of the last synced state file.
    - start_time (int): Start timestamp used for the job.
    - end_time (int): End timestamp used for the job.
    - interval (int): Scheduler interval (seconds).
    - delay (int): Scheduler delay (seconds).
    - run_now (bool): Whether the job ran immediately.
    - batch_size (int): Number of records processed per batch.
    - max_workers (int): Number of worker threads.
    - sources (list[str] | None): Data sources included in the run.

    **Raises:**
    - HTTPException 400: If the given chain is not supported.
    - HTTPException 500: If the deposits & users job fails during execution.
    """
    chain = payload.chain.lower()
    if chain not in Chains.mapping:
        raise HTTPException(status_code=400, detail=f"Chain '{chain}' is not supported")

    chain_id = Chains.mapping[chain]

    job = DepositsUsersScheduler(
        chain_id=chain_id,
        start_timestamp=payload.start_time,
        end_timestamp=payload.end_time,
        batch_size=payload.batch_size,
        max_workers=payload.max_workers,
        last_synced_file=payload.last_synced_file,
        sources=list(payload.source) if payload.source else None,
        run_now=payload.run_now,
        interval=payload.interval,
        delay=payload.delay,
        mongodb=mongodb,
        cassandra=clickhouse,
    )

    try:
        await to_thread(job.run)  # pyright: ignore [reportUnknownArgumentType,reportUnknownMemberType]
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Deposits & Users failed: {e}"
        ) from e

    return RunResult(
        status=RunStatus.COMPLETED,
        chain=chain,
        chain_id=chain_id,
        last_synced_file=payload.last_synced_file,
        start_time=payload.start_time,
        end_time=payload.end_time,
        interval=payload.interval,
        delay=payload.delay,
        run_now=payload.run_now,
        batch_size=payload.batch_size,
        max_workers=payload.max_workers,
        sources=list(payload.source) if payload.source else None,
    )


@app.post(
    "/graph/subgraph-exporter/run",
    response_model=RunResult,
    tags=[Configs.get_graph_tag()],
)
async def run_subgraph_exporter(payload: SubgraphExporterPayload):
    """
    Export subgraphs from ArangoDB to MongoDB using the given payload.

    **Request Parameters (SubgraphExporterPayload):**
    - chain (str): Blockchain name (must exist in `Chains.mapping`). Example: "ethereum".
    - radius (int, optional): Graph traversal radius (number of hops). Default = 2.
    - batch_size (int, optional): Number of addresses to process per batch. Default = 3600.
    - max_workers (int, optional): Number of worker threads. Default = 4.
    - start_time (int, optional): Start timestamp (UNIX epoch, seconds).
    - end_time (int, optional): End timestamp (UNIX epoch, seconds).
    - delay (int, optional): Delay before scheduler starts (seconds). Default = 0.
    - run_now (bool, optional): Whether to run immediately. Default = True.
    - last_synced_file (str, optional): Path to last synced file. Default = ".data/last_synced_subgraph_exporter.txt".

    **Returns (RunResult):**
    - status (str): Execution status ("completed" if successful).
    - chain (str): Blockchain name.
    - chain_id (int): Internal chain identifier.
    - radius (int): Graph traversal radius used in the job.
    - batch_size (int): Number of addresses processed per batch.
    - max_workers (int): Number of worker threads.
    - unique_addresses (int): Count of unique user wallet addresses exported.
    - start_time (int): Start timestamp used for the job.
    - end_time (int): End timestamp used for the job.
    - delay (int): Scheduler delay (seconds).
    - run_now (bool): Whether the job ran immediately.

    **Raises:**
    - HTTPException 400: If the given chain is not supported.
    - HTTPException 500: If the subgraph exporter job fails during execution.
    """
    chain = payload.chain.lower()
    if chain not in Chains.mapping:
        raise HTTPException(status_code=400, detail=f"Chain '{chain}' is not supported")
    chain_id = Chains.mapping[chain]

    def _do_work() -> int:
        arango = arangodb.get_arangodb_service(str(chain))

        cursor = mongodb.get_user_wallet_from_deposit_wallets(
            _filter={"chainId": chain_id},
            projection={"_id": 1, "userWallets": 1},
        )

        addresses: list[str] = []
        for doc in cursor.limit(100):
            addresses.extend(doc.get("userWallets", []))
        unique_addresses: list[str] = list(set(addresses))

        job = SubgraphExporterJob(
            importer=arango,
            exporter=mongodb,
            chain_id=chain_id,
            addresses=unique_addresses,
            radius=payload.radius,
            batch_size=payload.batch_size,
            max_workers=payload.max_workers,
        )
        job.run()
        return len(unique_addresses)

    try:
        unique_count = await to_thread(_do_work)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Subgraph exporter failed: {e}"
        ) from e

    return RunResult(
        status=RunStatus.COMPLETED,
        chain=chain,
        chain_id=chain_id,
        radius=payload.radius,
        batch_size=payload.batch_size,
        max_workers=payload.max_workers,
        unique_addresses=unique_count,
        start_time=payload.start_time,
        end_time=payload.end_time,
        delay=payload.delay,
        run_now=payload.run_now,
    )


# ---------- pipeline steps ----------
@app.post(
    "/data/query-subgraph/run",
    response_model=PipelineStepResp,
    tags=[Configs.get_data_tag()],
)
async def api_query_subgraph(req: QuerySubgraphReq) -> PipelineStepResp:
    """
    API: /query-subgraph
    Compute query subgraphs.

    Input (QuerySubgraphReq):
    - chain (str)
    - radius (int)
    - max_workers (int)


    Output (PipelineStepResp):
    - ok (bool), step="query-subgraph", seconds (float)
    - Mongo: subgraph_{chain}_{radius}_preprocessed
    """
    logger.info("[API] query_subgraph start: %s", req.describe())
    t0 = time.time()

    try:
        radius = req.radius
        max_workers = req.max_workers_cpu
        chain = req.chain.lower()
        if chain not in Chains.mapping:
            raise ValueError(f"Chain '{chain}' is not supported")

        logger.info("Getting token list")

        logger.info("Preparing subgraph → Mongo (pure Python)")
        out_col = f"subgraph_{chain}_{radius}_preprocessed"

        count, _ = query_subgraph_to_mongo(
            chain=chain,
            radius=radius,
            out_collection_name=out_col,
            unique_key="_id",
            max_vertices=200,
            client=mongodb,
            max_workers=max_workers,
        )
        logger.info(f"Prepared {count} subgraphs into '{out_col}'")

    except AppError as e:
        logger.error("AppError in query_subgraph : %s", e, exc_info=True)
        _raise_http(e)
    except Exception as e:
        logger.exception("query_subgraph  failed")
        _raise_http(AppError.from_exc(e, message="query_subgraph  failed"))
    secs = round(time.time() - t0, 2)
    logger.info("query_subgraph finished in %.2fs", secs)
    return PipelineStepResp(ok=RunStatus.COMPLETED, step="query-subgraph", seconds=secs)


@app.post(
    "/data/time-amount/run",
    response_model=PipelineStepResp,
    tags=[Configs.get_data_tag()],
)
async def api_time_amount(req: TimeAmountReq) -> PipelineStepResp:
    """
    API: /time-amount
    Compute time-series token transfer features for subgraphs.

    Input (TimeAmountReq):
    - chain (str)
    - chain_id (str)
    - list_index (list[str] | None)
    - token_list (list[str])
    - transaction_database (StandardDatabase)
    - mongo_db (MongoDB)
    - mongo_collection_prefix (str)
    - subgraph_collection_name (str)
    - mongo_query_filter (dict | None)
    - max_workers (int)
    - batch_size (int)

    Output (PipelineStepResp):
    - ok (bool), step="time-amount", seconds (float)
    - Mongo: {prefix}_subgraph, {prefix}_from, {prefix}_to
    """
    logger.info("[API] time_amount_exporter start: %s", req.describe())
    t0 = time.time()

    try:
        radius = req.radius
        batch_size = req.batch_size
        max_workers = req.max_workers
        chain = req.chain.lower()
        if chain not in Chains.mapping:
            raise ValueError(f"Chain '{chain}' is not supported")
        chain_id: str = Chains.mapping[chain]

        logger.info("Getting token list")
        token_list: list[str] = mongo_entity.get_top_token(chain_id=chain_id)

        logger.info("Preparing subgraph → Mongo (pure Python)")
        out_col = f"subgraph_{chain}_{radius}_preprocessed"

        # count, ids = query_subgraph_from_mongo_parallel(
        #     out_collection_name=out_col,
        #     parallelism=max_workers,
        #     client=mongodb,
        # )
        # logger.info(f"Prepared {count} subgraphs into '{out_col}'")

        arango = arangodb.get_arangodb_service(chain)

        job: TimeAmountExporterJob = TimeAmountExporterJob(
            chain=chain,
            chain_id=chain_id,
            list_index=None,
            token_list=token_list,
            transaction_database=arango.db,
            mongo_db=mongodb,
            mongo_collection_prefix="time_amount_features",
            subgraph_collection_name=out_col,
            mongo_query_filter={},
            max_workers=max_workers,
            batch_size=batch_size,
        )
        await to_thread(job.run)

    except AppError as e:
        logger.error("AppError in time_amount_exporter: %s", e, exc_info=True)
        _raise_http(e)
    except Exception as e:
        logger.exception("time_amount_exporter failed")
        _raise_http(AppError.from_exc(e, message="time_amount_exporter failed"))
    secs = round(time.time() - t0, 2)
    logger.info("time_amount_exporter finished in %.2fs", secs)
    return PipelineStepResp(ok=RunStatus.COMPLETED, step="time-amount", seconds=secs)


@app.post(
    "/data/deposit_reuse_pairs/run",
    response_model=PipelineStepResp,
    tags=[Configs.get_data_tag()],
)
async def api_deposit_reuse_pairs(req: DepositReusePairReq) -> PipelineStepResp:
    """
    API: /deposit_reuse_pairs
    Detect deposit wallets reused across users.

    Input (DepositReusePairReq):
    - chain (str)
    - mongo_db (MongoDB)
    - refresh_number_sent_received (bool, default=True)
    - pairs_collection_name (str | None)
    - max_workers (int, default=2)
    - batch_size (int, default=1000)

    Output (PipelineStepResp):
    - ok (bool), step="deposit_reuse_pairs", seconds (float)
    - Mongo: upserts {X_address, SubX_address, chain, deposit_address, updatedAt}
    """
    logger.info("[API] generate_deposit_reuse_pairs start: %s", req.describe())
    t0 = time.time()

    try:
        chain_name = req.chain.lower()
        if chain_name not in Chains.mapping:
            raise ValueError(f"Chain '{chain_name}' is not supported")
        pairs_collection_name = req.pairs_collection_name
        max_workers = req.max_workers
        batch_size = req.batch_size
        job = DepositReusePairJob(
            chain=chain_name,
            mongo_db=mongodb,
            refresh_number_sent_received=True,
            pairs_collection_name=pairs_collection_name,
            max_workers=max_workers,
            batch_size=batch_size,
        )
        await to_thread(job.run)

    except AppError as e:
        _raise_http(e)
    except Exception as e:
        logger.exception("generate_deposit_reuse_pairs failed")
        _raise_http(AppError.from_exc(e, message="generate_deposit_reuse_pairs failed"))
    secs = round(time.time() - t0, 2)
    logger.info("generate_deposit_reuse_pairs finished in %.2fs", secs)
    return PipelineStepResp(
        ok=RunStatus.COMPLETED, step="deposit_reuse_pairs", seconds=secs
    )


# TODO : Temporary don't use this node-embedding (save it in case use in the future)
# @app.post(
#     "/data/node-embedding/run",
#     response_model=PipelineStepResp,
#     tags=[Configs.get_data_tag()],
# )
# async def api_node_embedding(req: NodeEmbeddingReq) -> PipelineStepResp:
#     logger.info("[API] node_embedding_exporter start: %s", req.describe())
#     t0 = time.time()
#     try:
#         await to_thread(node_embedding_exporter, **req.to_function_kwargs())
#     except AppError as e:
#         _raise_http(e)
#     except Exception as e:
#         logger.exception("node_embedding_exporter failed")
#         _raise_http(AppError.from_exc(e, message="node_embedding_exporter failed"))
#     secs = round(time.time() - t0, 2)
#     logger.info("node_embedding_exporter finished in %.2fs", secs)
#     return PipelineStepResp(ok=True, step="node-embedding", seconds=secs)


@app.post(
    "/data/combine-features/run",
    response_model=PipelineStepResp,
    tags=[Configs.get_data_tag()],
)
async def api_combine_features(req: CombineFeaturesReq) -> PipelineStepResp:
    """
    API: /combine-features
    Join FROM/TO features (output from time-amount api) and pairs (deposit_reuse_pairs api) into a training dataset.

    Input (CombineFeaturesReq):
    - from_col_name (str)
    - to_col_name (str)
    - embedding_col_name (str)
    - pairs_col_name (str)
    - out_train_col_name (str)
    - out_test_col_name (str)
    - compute_embedding_similarity (bool, default=True)
    - train_ratio (float, default=0.9)
    - chain_id (str | None)
    - balance_train_by_label (bool, default=True)
    - random_seed (int, default=42)

    Output (PipelineStepResp):
    - ok (bool), step="combine-features", seconds (float)
    - Mongo: upserts TRAIN/TEST docs with features, labels, embeddings
    """
    logger.info("[API] combine_features_mongo start: %s", req.describe())
    t0 = time.time()

    try:
        chain = req.chain.lower()
        if chain not in Chains.mapping:
            raise ValueError(f"Chain '{chain}' is not supported")

        chain_id = Chains.mapping[chain]
        from_col_name = req.from_col_name
        to_col_name = req.to_col_name
        embedding_col_name = req.embedding_col_name
        pairs_col_name = req.pairs_col_name
        contracts_col_name = req.contracts_col_name
        out_train_col_name = req.out_train_col_name
        out_test_col_name = req.out_test_col_name
        compute_embedding_similarity = req.compute_embedding_similarity
        train_ratio = req.train_ratio
        balance_train_by_label = req.balance_train_by_label
        runner = ProcessTrainingDatasetMongo(
            db=mongodb.db,
            from_col_name=from_col_name,
            to_col_name=to_col_name,
            embedding_col_name=embedding_col_name,
            pairs_col_name=pairs_col_name,
            contracts_col_name=contracts_col_name,
            out_train_col_name=out_train_col_name,
            out_test_col_name=out_test_col_name,
            compute_embedding_similarity=compute_embedding_similarity,
            train_ratio=train_ratio,
            chain_id=chain_id,
            balance_train_by_label=balance_train_by_label,
        )
        await to_thread(runner.run)

    except AppError as e:
        _raise_http(e)
    except Exception as e:
        logger.exception("combine_features_mongo failed")
        _raise_http(AppError.from_exc(e, message="combine_features_mongo failed"))
    secs = round(time.time() - t0, 2)
    logger.info("combine_features_mongo finished in %.2fs", secs)
    return PipelineStepResp(
        ok=RunStatus.COMPLETED, step="combine-features", seconds=secs
    )


# ---------- training ----------
@app.post(
    "/train/from-mongo-to-txt-hf/run",
    response_model=TrainFromMongoResp,
    tags=[Configs.get_training_tag()],
)
async def train_from_mongo_to_txt_hf(req: TrainFromMongoReq) -> TrainFromMongoResp:  # pyright: ignore [reportReturnType]
    logger.info("[API] train_from_mongo_to_txt_hf start: %s", req.describe())

    db = mongodb.db

    try:
        paths = req.artifact_paths()
        await to_thread(os.makedirs, req.output_dir, exist_ok=True)

        n_train = await export_collection_to_csv_async(
            db,
            req.train_collection,
            paths["train_csv"],
        )
        n_test = await export_collection_to_csv_async(
            db,
            req.test_collection,
            paths["test_csv"],
        )

        trainer = LightGBMTrainer(
            drop_cols=req.drop_cols,
            smote_k=req.smote_k,
            num_leaves=req.num_leaves,
            feature_fraction=req.feature_fraction,
            max_depth=req.max_depth,
        )
        try:
            model, results = await to_thread(
                trainer.train_and_evaluate, paths["train_csv"], paths["test_csv"]
            )
        except ValueError as exc:
            _raise_http(DataValidationError("Invalid training data").with_cause(exc))
        except Exception as exc:
            _raise_http(TrainingError("LightGBM training failed").with_cause(exc))

        try:
            _ = await to_thread(model.save_model, paths["model_txt"])  # pyright: ignore [reportOptionalMemberAccess,reportPossiblyUnboundVariable]
        except Exception as exc:
            _raise_http(
                SerializationError("Failed to save LightGBM model")
                .with_context(path=paths["model_txt"])
                .with_cause(exc)
            )

        try:
            uploader = HuggingFaceUploader(
                token=None, org_name=None, private_default=req.hf_private
            )
            hf_url = await to_thread(
                uploader.upload_file,
                model_path=paths["model_txt"],
                repo_basename=req.hf_repo_basename,
                path_in_repo=os.path.basename(paths["model_txt"]),
                repo_type="model",
            )
            return TrainFromMongoResp(
                message=RunStatus.COMPLETED,
                rows={"train": n_train, "test": n_test},
                evaluation=results,  # pyright: ignore [reportPossiblyUnboundVariable]
                artifacts=paths,
                huggingface_model_url=hf_url,
            )
        except AppError as e:
            _raise_http(e)
        except Exception as exc:
            _raise_http(
                ExternalServiceError("Hugging Face upload failed").with_cause(exc)
            )

    except HTTPException:
        raise
    except AppError as e:
        _raise_http(e)
    except Exception as e:
        logger.exception("train_from_mongo_to_txl_hf failed")
        _raise_http(AppError.from_exc(e, message="Training endpoint failed"))


FlowName = Literal[
    "graph_exporter",
    "graph_prune",
    "exchange_deposit_wallets",
    "deposits_and_users",
    "subgraph_exporter",
]

DEFAULT_ORDER: list[FlowName] = [
    "graph_exporter",
    "graph_prune",
    "exchange_deposit_wallets",
    "deposits_and_users",
    "subgraph_exporter",
]


class OrchestratorRequest(BaseModel):
    """
    /run-all request:
      - `common`: required shared CommonParams applied to every flow (wins for shared fields)
      - per-flow objects are OVERRIDES only, so the schema won't expose common fields again
    """

    order: list[FlowName] | None = DEFAULT_ORDER
    common: CommonParams

    graph_exporter: GraphExporterOverrides | None = None
    graph_prune: GraphPruneOverrides | None = None
    exchange_deposit_wallets: ExchangeDepositWalletsOverrides | None = None
    deposits_and_users: DepositsAndUsersOverrides | None = None
    subgraph_exporter: SubgraphExporterOverrides | None = None


class OrchestratorResponse(BaseModel):
    """
    Response schema for /run-all endpoint.
    """

    status: RunStatus
    order: list[FlowName]
    results: dict[FlowName, RunResult]


T = TypeVar("T", bound=BaseModel)
OverrideType = TypeVar("OverrideType", bound=BaseModel)


def _merge_payload(
    *,
    common: CommonParams,
    overrides: BaseModel | None,
    target_cls: type[T],
) -> T:
    """
    Build final per-flow payload of type `target_cls` from:
      1) target model defaults
      2) common (highest priority for shared fields)
      3) job-specific overrides
    """
    # 1) start from the target payload defaults
    base = target_cls().model_dump(exclude_none=True)

    # 2) overlay common (wins for shared fields)
    final_dict: dict[str, Any] = {**base, **common.model_dump(exclude_none=True)}

    # 3) overlay overrides (job-specific only)
    if overrides:
        final_dict.update(overrides.model_dump(exclude_none=True))

    return target_cls(**final_dict)


# ---- Single orchestrator endpoint ----
@app.post(
    "/all/all-graph/run",
    response_model=OrchestratorResponse,
    response_model_exclude_none=True,
    tags=[Configs.get_all_tag()],
)
async def run_all(req: OrchestratorRequest):
    """
    Run multiple graph-related flows sequentially with shared common parameters and optional per-flow overrides.

    **Request Parameters (OrchestratorRequest):**
    - order (list[str] | None, optional): List of flows to run in sequence. If omitted, defaults to:
        ["graph_exporter", "graph_prune", "exchange_deposit_wallets", "deposits_and_users", "subgraph_exporter"].
    - common (CommonParams): Required shared parameters applied to every flow (highest priority for shared fields).
    - graph_exporter (GraphExporterOverrides | None, optional): Job-specific overrides for Graph Exporter.
    - graph_prune (GraphPruneOverrides | None, optional): Job-specific overrides for Graph Prune.
    - exchange_deposit_wallets (ExchangeDepositWalletsOverrides | None, optional): Job-specific overrides for Exchange Deposit Wallets.
    - deposits_and_users (DepositsAndUsersOverrides | None, optional): Job-specific overrides for Deposits & Users.
    - subgraph_exporter (SubgraphExporterOverrides | None, optional): Job-specific overrides for Subgraph Exporter.

    **Returns (OrchestratorResponse):**
    - status (str): Execution status ("completed" if all flows ran successfully).
    - order (list[str]): The actual order of flows executed.
    - results (dict[str, RunResult]): Mapping of flow name to its execution result (`RunResult` object).

    **Raises:**
    - HTTPException 400: If a flow name in `order` is unknown.
    - HTTPException 500: If any flow fails during execution.
    """
    order = DEFAULT_ORDER
    results: dict[FlowName, RunResult] = {}

    # Build final payloads: common wins; overrides add job-specific knobs
    p_ge = _merge_payload(
        common=req.common, overrides=req.graph_exporter, target_cls=GraphExporterPayload
    )
    p_gp = _merge_payload(
        common=req.common, overrides=req.graph_prune, target_cls=GraphPrunePayload
    )
    p_edw = _merge_payload(
        common=req.common,
        overrides=req.exchange_deposit_wallets,
        target_cls=ExchangeDepositWalletsPayload,
    )
    p_du = _merge_payload(
        common=req.common,
        overrides=req.deposits_and_users,
        target_cls=DepositsAndUsersPayload,
    )
    p_se = _merge_payload(
        common=req.common,
        overrides=req.subgraph_exporter,
        target_cls=SubgraphExporterPayload,
    )

    # Execute sequentially (await your async handlers)
    for flow in order:
        try:
            if flow == "graph_exporter":
                results[flow] = await run_graph_exporter(p_ge)
            elif flow == "graph_prune":
                results[flow] = await run_graph_prune(p_gp)
            elif flow == "exchange_deposit_wallets":
                results[flow] = await run_exchange_deposit_wallets(p_edw)
            elif flow == "deposits_and_users":
                results[flow] = await run_deposits_and_users(p_du)
            elif flow == "subgraph_exporter":
                results[flow] = await run_subgraph_exporter(p_se)
            else:
                raise HTTPException(status_code=400, detail=f"Unknown flow '{flow}'")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Flow '{flow}' failed: {e}"
            ) from e

    return OrchestratorResponse(
        status=RunStatus.COMPLETED, order=order, results=results
    )


@app.post(
    "/all/data-collection/run", response_model=RunAllResp, tags=[Configs.get_all_tag()]
)
async def api_run_all(req: RunAllReq) -> RunAllResp:  # pyright: ignore [reportReturnType]
    try:
        _ = await api_time_amount(req.time_amount)
        _ = await api_deposit_reuse_pairs(req.deposit_reuse_pairs)
        # await api_node_embedding(req.node_embedding)  # optional
        _ = await api_combine_features(req.combine_features)
        return RunAllResp(ok=RunStatus.COMPLETED, seconds=0.0)

    except AppError as e:
        logger.error("run-all failed with AppError: %s", e, exc_info=True)
        _raise_http(e)
    except Exception as e:
        logger.exception("run-all failed")
        _raise_http(AppError.from_exc(e, message="run-all failed"))


# ---------- run-all + train ----------
@app.post(
    "/all/data-collection-and-train/run",
    response_model=RunAllAndTrainResp,
    tags=[Configs.get_all_tag()],
)
async def api_run_all_and_train(req: RunAllAndTrainReq) -> RunAllAndTrainResp:
    """
    API: /data-collection-and-train
    Run the full pipeline: time-amount → deposit_reuse_pairs → combine-features → train-from-mongo-to-txt-hf.

    Input (RunAllAndTrainReq):
    - chain (str)
    - chain_id (str)
    - list_index (list[str] | None)
    - token_list (list[str])
    - transaction_database (StandardDatabase)
    - mongo_db (MongoDB)
    - mongo_collection_prefix (str)
    - subgraph_collection_name (str)
    - mongo_query_filter (dict | None)
    - max_workers (int)
    - batch_size (int)
    - refresh_number_sent_received (bool, default=True)
    - pairs_collection_name (str | None)
    - from_col_name (str)
    - to_col_name (str)
    - embedding_col_name (str)
    - pairs_col_name (str)
    - contracts_col_name (str | None)
    - out_train_col_name (str)
    - out_test_col_name (str)
    - compute_embedding_similarity (bool, default=True)
    - train_ratio (float, default=0.9)
    - balance_train_by_label (bool, default=True)
    - random_seed (int, default=42)
    - drop_cols (list[str] | None)
    - smote_k (int | None)
    - hf_repo_basename (str)
    - hf_private (bool)
    - output_dir (str)

    Output (RunAllAndTrainResp):
    - ok (bool)
    - steps (dict[str, PipelineStepResp | TrainFromMongoResp])
    - seconds (float)
    """
    try:
        logger.info("[API] run-all-and-train start: %s", req.describe())
        _ = await api_time_amount(req.pipeline.time_amount)
        _ = await api_deposit_reuse_pairs(req.pipeline.deposit_reuse_pairs)
        # await api_node_embedding(req.node_embedding)  # optional
        _ = await api_combine_features(req.pipeline.combine_features)
        resp = await train_from_mongo_to_txt_hf(req.training)

    except AppError as e:
        logger.error("run-all-and-train failed with AppError: %s", e, exc_info=True)
        _raise_http(e)
    except Exception as e:
        logger.exception("run-all-and-train failed")
        _raise_http(AppError.from_exc(e, message="run-all-and-train failed"))
    logger.info("run-all-and-train completed successfully")
    return resp  # pyright: ignore [reportPossiblyUnboundVariable,reportReturnType]
