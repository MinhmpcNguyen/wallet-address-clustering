from typing import NotRequired, TypedDict

from bson import ObjectId


class LPTokenDoc(TypedDict):
    _id: ObjectId
    name: str
    symbol: str
    decimals: NotRequired[int]


class DepositWalletDoc(TypedDict):
    _id: ObjectId
    address: str
    user_id: str
    created_at: NotRequired[int]


class GroupDoc(TypedDict):
    _id: ObjectId
    name: str


class DepositUserDoc(TypedDict):
    _id: ObjectId
    user_id: str
    wallets: NotRequired[list[str]]
    updating: NotRequired[int]
    numUsers: NotRequired[int]
    userWallets: list[str]


class UserDepositDoc(TypedDict):
    _id: ObjectId
    user_id: str
    tx_hash: str
    amount: float
    timestamp: int


class TransactionDoc(TypedDict):
    _id: ObjectId
    hash: str
    # Use 'from_' to avoid Python keyword conflict while keeping Mongo field 'from'
    from_: str
    to: str
    value: int
    timestamp: int


class TokenTransferDoc(TypedDict):
    _id: ObjectId
    tx_hash: str
    token: str
    from_: str
    to: str
    amount: str
    timestamp: int


class SubgraphEdge(TypedDict):
    from_: str
    to: str


class TokenTransferLogs(TypedDict):
    # token -> { timestamp -> { amount, valueInUSD } }
    # Example:
    # {
    #   "USDT": {
    #       "1690000000": {"amount": 100, "valueInUSD": 100.5},
    #       ...
    #   },
    #   ...
    # }
    __root__: dict[str, dict[str, dict[str, float | int]]]


class SubgraphDoc(TypedDict):
    _id: ObjectId | str
    # usually list of {from, to}; keep as dict[str, str] for flexibility
    edges: NotRequired[list[dict[str, str]]]
    vertices: NotRequired[list[SubgraphEdge]]
    chain: NotRequired[str]
    chainId: NotRequired[str]
    tokenTransferLogs: NotRequired[dict[str, dict[str, dict[str, float | int]]]]
