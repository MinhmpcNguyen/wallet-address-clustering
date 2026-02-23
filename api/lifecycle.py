# api/lifecycle.py
from contextlib import asynccontextmanager

from clients import Clients
from fastapi import FastAPI
from utils.logger_utils import get_logger

logger = get_logger("Lifecycle")


async def _startup() -> None:
    """
    Initialize shared clients once per process at application startup.
    - ClickHouse and MongoDB can be pre-initialized (no arguments required).
    - Arango requires a chain_id, so we keep it lazy (init on demand in handlers).
    """
    # Ensure singletons are constructed and cached by their wrappers
    _ = Clients.get_clickhouse_client().get_clickhouse_service()
    _ = Clients.get_mongo_client().get_mongodb_service()
    _ = Clients.get_mongo_entity_client().get_mongodb_entity_service()


async def _shutdown() -> None:
    """
    Cleanly close clients at shutdown if your implementations expose close() methods.
    """

    logger.info("App shutting down: closing clients...")
    try:
        Clients.close_all()
    except Exception as e:
        logger.error("Error during shutdown close_all: %s", e, exc_info=True)
    logger.info("Shutdown complete.")
    pass


@asynccontextmanager
async def lifespan(app: FastAPI):  # pyright: ignore[reportUnusedParameter]
    """
    FastAPI lifespan – runs on startup and shutdown.
    You can optionally expose client singletons via app.state for convenience.
    """
    await _startup()

    try:
        yield
    finally:
        await _shutdown()


@asynccontextmanager
async def headless_lifespan():
    """
    Headless lifespan for schedulers/CLI scripts that need the same init/teardown
    without a FastAPI app instance.
    """
    await _startup()
    try:
        yield
    finally:
        await _shutdown()
