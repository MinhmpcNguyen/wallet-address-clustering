from typing import TypedDict

from bson import ObjectId
from pydantic import BaseModel


class DepositData(TypedDict):
    _key: str
    address: str
    numberSent: int
    numberReceived: int


class DepositReusePairDoc(TypedDict, total=False):
    _id: ObjectId | str
    X_address: str
    chainId: str
    SubX_address: str
    chain: str
    deposit_address: str
    updatedAt: int


class RawTokenFlowModel(BaseModel):
    subgraphId: str
    address: str
    token: str
    time: list[str]
    amount: list[float | int]
    valueInUSD: list[float | int]


class SubgraphDoc(TypedDict):
    _id: str
    chain: str
    chainId: str
    vertices: list[dict[str, str]]
