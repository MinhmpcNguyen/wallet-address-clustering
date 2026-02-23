from typing import TypedDict


class FromToDoc(TypedDict):
    subgraphId: str
    address: str
    token: str
    time: list[int]  # epoch timestamps
    amount: list[float | str]  # amounts can be stored as strings
    valueInUSD: list[float]
