from typing import NotRequired, TypedDict


class ConfigDoc(TypedDict, total=False):
    _id: str
    # add concrete config fields here as needed
    # e.g. name: str; value: str


class MultichainWalletDoc(TypedDict, total=False):
    _id: str
    # e.g. "wallets": list[dict[str, str]]


class PriceChangePoint(TypedDict):
    # adjust according to your real structure
    ts: int  # timestamp
    price: float  # price at ts
    # add other fields if present


class SmartContractDoc(TypedDict):
    _id: str  # f"{chainId}_{address}"
    address: str
    chainId: NotRequired[str]
    idCoingecko: NotRequired[str]
    priceChangeLogs: PriceChangePoint


# Narrow view when projecting priceChangeLogs (Mongo keeps _id unless excluded)
class SmartContractPriceChangeDoc(TypedDict, total=False):
    _id: str
    priceChangeLogs: list[PriceChangePoint]
