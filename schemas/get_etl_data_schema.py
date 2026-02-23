from pydantic import BaseModel, Field, field_validator

# Import your existing function
from constants.network_constants import Chains


class GraphExporterRequest(BaseModel):
    last_synced_file: str = Field(
        ".data/last_synced_graph_exporter.txt",
        description="Path to file storing last synced timestamp",
    )
    start_time: int | None = Field(
        1736182800, description="Start UNIX timestamp (seconds)"
    )
    end_time: int | None = Field(1752512400, description="End UNIX timestamp (seconds)")
    period: int = Field(
        900, description="Batch size in seconds for each worker (e.g., 900 = 15m)"
    )
    max_workers: int = Field(4, description="Number of worker threads/processes")
    chain: str = Field("ethereum", description="Chain name, e.g., ethereum, bsc")
    interval: int = Field(
        259200, description="Interval window in seconds (e.g., 259200 = 3 days)"
    )
    delay: int = Field(0, description="Delay (seconds) before each scheduled run")
    run_now: bool = Field(True, description="Run immediately on start")
    source: list[str] | None = Field(
        None, description="Sources (e.g., ['transactions','token_transfers'])"
    )

    @field_validator("chain")
    def validate_chain(cls, v: str) -> str:
        v_lower = (v or "").lower()
        if v_lower not in Chains.mapping:
            raise ValueError(
                f"Unsupported chain '{v}'. Supported: {list(Chains.mapping.keys())}"
            )
        return v_lower

    @field_validator("period", "interval", "delay", "max_workers")
    def positive_ints(cls, v: int) -> int:
        if v < 0:
            raise ValueError("Must be >= 0")
        return v
