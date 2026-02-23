from typing import Any, final

from pydantic import BaseModel


class Metadata(BaseModel):
    """Pydantic model for tag metadata containing name and description."""

    name: str
    description: str


@final
class TagsMetadata:
    """Final class containing all tag metadata constants for the API endpoints."""

    GRAPH = Metadata(
        name="GRAPH", description="Graph endpoints for graph database operations."
    )
    DATA = Metadata(name="DATA", description="Data endpoints for data processing.")
    TRAINING = Metadata(
        name="TRAINING", description="Training endpoints for model training."
    )
    ALL = Metadata(name="ALL", description="Endpoints that run all operations.")


class Configs:
    """Configuration class providing static methods to access tag names and metadata."""

    @staticmethod
    def get_graph_tag() -> str:
        """Get the Graph tag name."""
        return TagsMetadata.GRAPH.name

    @staticmethod
    def get_data_tag() -> str:
        """Get the Data tag name."""
        return TagsMetadata.DATA.name

    @staticmethod
    def get_training_tag() -> str:
        """Get the Training tag name."""
        return TagsMetadata.TRAINING.name

    @staticmethod
    def get_all_tag() -> str:
        """Get the All tag name."""
        return TagsMetadata.ALL.name

    @staticmethod
    def get_tags_metadata() -> list[dict[str, Any]]:
        """
        Get all tags metadata as a list of dictionaries.

        Returns:
            list[dict[str, Any]]: List of tag metadata dictionaries suitable for FastAPI.
        """
        tags_metadata = [
            TagsMetadata.GRAPH.model_dump(),
            TagsMetadata.DATA.model_dump(),
            TagsMetadata.TRAINING.model_dump(),
        ]
        return tags_metadata


# Export commonly used items for easier imports
__all__ = ["Metadata", "TagsMetadata", "Configs"]
