from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field

from constants.time_constants import TimeConstants


class GraphExporterOverrides(BaseModel):
    """
    Overrides for Graph Exporter flow when running via /run-all.

    **Fields:**
    - period (int, optional): Batch window in seconds for exporter job. Default = 900.
    - source (list[str] | None, optional): Data sources to export (e.g., ["transactions", "token_transfers"]). Default = [].
    - last_synced_file (str, optional): Path to last synced file. Default = ".data/last_synced_graph_exporter.txt".
    - interval (int, optional): Scheduler interval in seconds. Default = 86400 (1 day).
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    job_tag: ClassVar[str] = "graph_exporter"

    period: int = Field(900, description="Batch window in seconds for exporter job")

    source: list[str] | None = Field(
        None,
        description="['transactions', 'token_transfers', ...]",
        json_schema_extra={"example": []},
    )
    # Inherit the rest from CommonParams; override any default by redeclaring here if needed.
    last_synced_file: str = ".data/last_synced_graph_exporter.txt"

    interval: int = Field(
        TimeConstants.A_DAY, description="Scheduler interval (seconds)"
    )


class GraphPruneOverrides(BaseModel):
    """
    Overrides for Graph Prune flow when running via /run-all.

    **Fields:**
    - timespan (int, optional): How many intervals of data to keep. Default = 120.
    - batch_size_query (int, optional): Batch size for database queries. Default = 100,000.
    - batch_size_thread (int, optional): Batch size for worker threads. Default = 100.
    - last_synced_file (str, optional): Path to last synced file. Default = ".data/0x1_graph_prune.txt".
    - interval (int, optional): Scheduler interval in seconds. Default = 86400 (1 day).
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    job_tag: ClassVar[str] = "graph_prune"

    timespan: int = Field(120, description="How many intervals to keep")
    batch_size_query: int = Field(100_000, ge=1)
    batch_size_thread: int = Field(100, ge=1)

    last_synced_file: str = ".data/0x1_graph_prune.txt"
    interval: int = Field(
        TimeConstants.A_DAY, description="Scheduler interval (seconds)"
    )


class ExchangeDepositWalletsOverrides(BaseModel):
    """
    Overrides for Exchange Deposit Wallets flow when running via /run-all.

    **Fields:**
    - period (int, optional): Per-worker slice in seconds. Default = 3600.
    - source (list[str] | None, optional): Data sources to include (e.g., ["transactions"]). Default = [].
    - last_synced_file (str, optional): Path to last synced file. Default = ".data/last_synced_deposit_wallets.txt".
    - interval (int, optional): Scheduler interval in seconds. Default = 86400 (1 day).
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    job_tag: ClassVar[str] = "exchange_deposit_wallets"

    period: int = Field(3600, description="Per-worker slice seconds")

    source: list[str] | None = Field(
        None,
        json_schema_extra={"example": []},
    )
    last_synced_file: str = ".data/last_synced_deposit_wallets.txt"
    interval: int = Field(
        TimeConstants.A_DAY, description="Scheduler interval (seconds)"
    )


class DepositsAndUsersOverrides(BaseModel):
    """
    Overrides for Deposits & Users flow when running via /run-all.

    **Fields:**
    - batch_size (int, optional): Number of records to process per batch. Default = 1000.
    - source (list[str] | None, optional): Data sources to include (e.g., ["transactions", "deposits"]). Default = [].
    - interval (int, optional): Scheduler interval in seconds. Default = 3600 (1 hour).
    - start_time (int, optional): Start timestamp (UNIX epoch, seconds). Default = 1755799200 (override for demo).
    - last_synced_file (str, optional): Path to last synced file. Default = ".data/last_synced_deposits_users.txt".
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    job_tag: ClassVar[str] = "deposits_and_users"

    batch_size: int = Field(1000, ge=1)

    source: list[str] | None = Field(
        None,
        json_schema_extra={"example": []},
    )
    interval: int = Field(
        3600, description="Roll every hour by default"
    )  # override default
    start_time: int = 1755799200  # override default
    last_synced_file: str = (
        ".data/last_synced_deposits_users.txt"  # override default to allow auto-gen
    )


class SubgraphExporterOverrides(BaseModel):
    """
    Overrides for Subgraph Exporter flow when running via /run-all.

    **Fields:**
    - radius (int, optional): Graph traversal radius (number of hops). Default = 2. Allowed range: [1, 6].
    - batch_size (int, optional): Number of addresses to process per batch. Default = 100.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    radius: int | None = Field(2, ge=1, le=6)
    batch_size: int | None = Field(100, ge=1)
