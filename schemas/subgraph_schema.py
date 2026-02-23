from typing import NotRequired, TypedDict


class EdgeDoc(TypedDict):
    from_: str
    to: str


class SubgraphDoc(TypedDict):
    _id: str
    address: str
    chainId: str
    edges: list[dict[str, str]]
    lastUpdatedAt: NotRequired[int]


class OutSubgraphDoc(TypedDict, total=False):
    # Original fields (may or may not exist)
    _id: str
    chain: str
    chainId: str
    lastUpdatedAt: int

    # Added/modified by preprocessing
    X_address: str
    edges: list[dict[str, str]]
    vertices: list[str]

    # Required field produced by your preprocessing
    NumAddress: int
