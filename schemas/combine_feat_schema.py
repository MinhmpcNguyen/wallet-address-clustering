# schemas/train_pair_schema.py
from typing import NotRequired, TypedDict


class TrainPairDoc(TypedDict, total=False):
    """
    Document stored in col_train / col_test after _write_output().
    Dynamic token features (e.g., 'X_From_USDT', 'SubX_To_ETH', …) are also
    present at the top level as floats, but cannot be pattern-typed in TypedDict.
    """

    _id: str  # ObjectId-like
    X_address: str
    SubX_address: str
    X_Time: list[int]  # length 24
    SubX_Time: list[int]  # length 24
    Diff2_Vec_Simi: NotRequired[float]
    Label: bool
    chainId: NotRequired[str]
    updatedAt: int  # unix timestamp
