from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field
from schemas.get_graph_schema import RunStatus

# ==============================
# Base / Mixins
# ==============================


class AppBaseModel(BaseModel):
    """
    Base for all API schemas.
    - v2 config: forbids extra fields by default.
    - Adds common helpers (to_kwargs, describe).
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid", frozen=False)

    def to_kwargs(
        self, exclude_none: bool = True, exclude: set[str] | None = None
    ) -> dict[str, Any]:
        """
        Convert this model to a kwargs dict that can be passed to underlying functions.
        """
        return self.model_dump(exclude=exclude, exclude_none=exclude_none)

    def describe(self) -> str:
        """
        Human-friendly summary, useful for logging.
        """
        return self.model_dump_json()


class WorkerConfigMixin(AppBaseModel):
    """
    Shared worker configuration used by exporters/generators.
    """

    batch_size: int = Field(default=100, ge=1)
    max_workers: int = Field(default=4, ge=1)


class ChainScopeMixin(AppBaseModel):
    """
    Shared chain context.
    """

    chain: str = Field(default="ethereum", description="Chain name, e.g. ethereum")


class HealthResp(AppBaseModel):
    ok: bool = True
    ts: int


class PipelineStepResp(AppBaseModel):
    ok: RunStatus
    step: str
    seconds: float


class RunAllResp(AppBaseModel):
    ok: RunStatus
    seconds: float


# ==============================
# Domain: Pipeline (per-stage)
# ==============================


class TimeAmountReq(WorkerConfigMixin, ChainScopeMixin):
    radius: int = Field(default=2, ge=1)
    batch_size: int = Field(default=100, ge=1)
    max_workers: int = Field(default=4, ge=1)
    chain: str = Field(default="ethereum")
    # def to_function_kwargs(self) -> dict[str, str | float | int]:
    #     """
    #     Map schema → time_amount_exporter(**kwargs)
    #     """
    #     return {
    #         "chain": self.chain,
    #         "radius": self.radius,
    #         "batch_size": self.batch_size,
    #         "max_workers": self.max_workers,
    #     }


class QuerySubgraphReq(WorkerConfigMixin, ChainScopeMixin):
    radius: int = Field(default=2, ge=1)
    batch_size: int = Field(default=100, ge=1)
    max_workers_cpu: int = Field(default=4, ge=1)
    chain: str = Field(default="ethereum")


class DepositReusePairReq(WorkerConfigMixin, ChainScopeMixin):
    pairs_collection_name: str = "deposit_reuse_pairs_ethereum"
    batch_size: int = Field(default=100, ge=1)
    max_workers: int = Field(default=4, ge=1)
    chain: str = Field(default="ethereum")
    # def to_function_kwargs(self) -> dict[str, str | int]:
    #     """
    #     Map schema → generate_deposit_reuse_pairs(**kwargs)
    #     """
    #     return {
    #         "chain_name": self.chain,
    #         "pairs_collection_name": self.pairs_collection_name,
    #         "batch_size": self.batch_size,
    #         "max_workers": self.max_workers,
    #     }


class NodeEmbeddingReq(ChainScopeMixin):
    out_collection_name: str = "subgraph_ethereum_2_preprocessed"
    dest_collection_name: str = "node_embeddings_ethereum_2"
    radius: int = Field(default=2, ge=1)

    def to_function_kwargs(self) -> dict[str, str | int]:
        """
        Map schema → node_embedding_exporter(**kwargs)
        """
        return {
            "out_collection_name": self.out_collection_name,
            "dest_collection_name": self.dest_collection_name,
            "chain": self.chain,
            "radius": self.radius,
        }


class CombineFeaturesReq(ChainScopeMixin):
    from_col_name: str = "time_amount_features_from"
    to_col_name: str = "time_amount_features_to"
    embedding_col_name: str = "node_embeddings_ethereum_2"
    pairs_col_name: str = "deposit_reuse_pairs_ethereum"
    contracts_col_name: str | None = None

    out_train_col_name: str = "train_data_ethereum_2"
    out_test_col_name: str = "test_data_ethereum_2"

    compute_embedding_similarity: bool = False
    train_ratio: float = Field(default=0.9, ge=0.05, le=0.99)
    balance_train_by_label: bool = True

    chain: str = Field(default="ethereum")

    def to_function_kwargs(self) -> dict[str, Any]:
        """
        Map schema → combine_features_mongo(**kwargs)
        """
        return {
            "from_col_name": self.from_col_name,
            "to_col_name": self.to_col_name,
            "embedding_col_name": self.embedding_col_name,
            "pairs_col_name": self.pairs_col_name,
            "contracts_col_name": self.contracts_col_name,
            "out_train_col_name": self.out_train_col_name,
            "out_test_col_name": self.out_test_col_name,
            "chain": self.chain,
            "compute_embedding_similarity": self.compute_embedding_similarity,
            "train_ratio": self.train_ratio,
            "balance_train_by_label": self.balance_train_by_label,
        }


class RunAllReq(AppBaseModel):
    """
    Complete pipeline: time_amount → deposit_reuse_pairs → node_embedding → combine_features
    """

    time_amount: TimeAmountReq = TimeAmountReq()
    deposit_reuse_pairs: DepositReusePairReq = DepositReusePairReq()
    node_embedding: NodeEmbeddingReq = NodeEmbeddingReq()
    combine_features: CombineFeaturesReq = CombineFeaturesReq()
    background: bool = False


# ==============================
# Domain: Training
# ==============================


class TrainFromMongoReq(AppBaseModel):
    train_collection: str = "train_data_ethereum_2"
    test_collection: str = "test_data_ethereum_2"

    drop_cols: list[str] = ["Unnamed: 0", "Diff2_Vec_Simi"]
    smote_k: int = 5
    num_leaves: int = 190
    feature_fraction: float = 0.4
    max_depth: int = 40
    output_dir: str = "output"
    model_txt_name: str = "lightgbm_model.txt"
    train_csv_name: str = "train_data.csv"
    test_csv_name: str = "test_data.csv"

    hf_repo_basename: str
    hf_private: bool = False

    # Derived paths helper (does not affect validation)
    def artifact_paths(self) -> dict[str, str]:
        return {
            "train_csv": f"{self.output_dir.rstrip('/')}/{self.train_csv_name}",
            "test_csv": f"{self.output_dir.rstrip('/')}/{self.test_csv_name}",
            "model_txt": f"{self.output_dir.rstrip('/')}/{self.model_txt_name}",
        }


class TrainFromMongoResp(AppBaseModel):
    message: RunStatus
    rows: dict[str, int]
    evaluation: dict[str, Any]
    artifacts: dict[str, str]
    huggingface_model_url: str


# ==============================
# Domain: Combo (Pipeline + Training)
# ==============================


class RunAllAndTrainReq(AppBaseModel):
    pipeline: RunAllReq
    training: TrainFromMongoReq
    background: bool = False


class RunAllAndTrainResp(AppBaseModel):
    message: str
    pipeline_seconds: float
    training: TrainFromMongoResp
