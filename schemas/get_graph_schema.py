from __future__ import annotations

from enum import Enum
from typing import ClassVar

from constants.time_constants import TimeConstants
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from utils.logger_utils import get_logger

logger = get_logger("Orchestrator API")


# =========================
# Common request primitives
# =========================


class CommonParams(BaseModel):
    """
    Base parameters shared across all jobs.
    Each payload inherits this to get defaults and validators.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    chain: str = Field(
        "ethereum", description="Human-readable name, e.g., ethereum / bsc / polygon"
    )
    start_time: int = Field(1755972000, description="Unix seconds (inclusive)")
    end_time: int = Field(1756144800, description="Unix seconds (exclusive)")
    # interval: int = Field(
    #     TimeConstants.A_DAY, description="Scheduler interval (seconds)"
    # )
    delay: int = Field(0, description="Schedule delay (seconds)")
    run_now: bool = Field(True, description="Run immediately at start")

    max_workers: int = Field(4, ge=1, le=64, description="Max parallel workers")

    @field_validator("chain", mode="before")
    @classmethod
    def _lower(cls, v: str) -> str:
        return str(v).lower()


# ===== 2) MIXIN TO AUTO-GENERATE last_synced_file =====
class WithDerivedLastSynced(CommonParams):
    job_tag: ClassVar[str] = "common"

    @model_validator(mode="after")
    def _autofill_last_synced(self):
        # Only touch if the model actually has this field
        if "last_synced_file" in self.__class__.model_fields:
            cur = getattr(self, "last_synced_file", None)
            if not cur:
                setattr(
                    self, "last_synced_file", f".data/{self.chain}_{self.job_tag}.txt"
                )
        return self


# ===== 3) SPECIFIC PAYLOADS =====
class GraphExporterPayload(WithDerivedLastSynced):
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


class GraphPrunePayload(WithDerivedLastSynced):
    job_tag: ClassVar[str] = "graph_prune"

    timespan: int = Field(120, description="How many intervals to keep")
    batch_size_query: int = Field(100_000, ge=1)
    batch_size_thread: int = Field(100, ge=1)

    last_synced_file: str = ".data/0x1_graph_prune.txt"
    interval: int = Field(
        TimeConstants.A_DAY, description="Scheduler interval (seconds)"
    )


class ExchangeDepositWalletsPayload(WithDerivedLastSynced):
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


class DepositsAndUsersPayload(WithDerivedLastSynced):
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


class SubgraphExporterPayload(WithDerivedLastSynced):
    job_tag: ClassVar[str] = "subgraph_exporter"

    # The original four fields
    chain: str = "ethereum"  # -c
    radius: int = 2  # -r
    batch_size: int = 3600  # -b

    # Whitelists for exporting to specific consumers
    JOB_FIELDS: ClassVar[set[str]] = {"chain", "radius", "batch_size", "max_workers"}
    SCHEDULER_FIELDS: ClassVar[set[str]] = {
        "start_time",
        "end_time",
        "interval",
        "delay",
        "run_now",
        "last_synced_file",
    }

    def job_kwargs(self) -> dict[str, str | int]:
        """Only the 4 original fields expected by the exporter job."""
        return self.model_dump(include=self.JOB_FIELDS)

    def scheduler_kwargs(self) -> dict[str, str | int]:
        """Only scheduling/runner fields (exclude job-specific knobs)."""
        return self.model_dump(include=self.SCHEDULER_FIELDS)


class RunStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    RUNNING = "running"
    PENDING = "pending"


class RunResult(BaseModel):
    status: RunStatus
    chain: str
    chain_id: str
    last_synced_file: str | None = None
    start_time: int | None
    end_time: int | None
    interval: int | None = None
    delay: int
    run_now: bool
    period: int | None = None
    max_workers: int
    sources: list[str] | None = None
    batch_size_query: int | None = None
    timespan: int | None = None
    batch_size_thread: int | None = None
    batch_size: int | None = None
    radius: int | None = None
    unique_addresses: int | None = None
