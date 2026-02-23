from typing_extensions import override

from constants.tag_constants import WalletTags
from models.wallet.wallet import Wallet


class WalletDepositExchange(Wallet):
    def __init__(self, address: str, last_updated_at: int):
        super().__init__(address, last_updated_at)
        self.add_tags(WalletTags.cex_deposit_wallet)

    @override
    def to_dict(self):
        returned_dict: dict[
            str, str | list[str] | int | dict[str, list[dict[str, str]]]
        ] = super().to_dict()
        exchanges = returned_dict.pop("protocols")
        returned_dict["depositedExchanges"] = {  # pyright: ignore [reportArgumentType]
            cex_id: [depl["chainId"] for depl in cex_deployments]  # pyright: ignore [reportUnknownVariableType]
            for cex_id, cex_deployments in exchanges.items()  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType,reportAttributeAccessIssue]
        }

        return returned_dict

    def to_dict_single_chain(self):
        returned_dict: dict[
            str, str | list[str] | int | dict[str, list[dict[str, str]]]
        ] = super().to_dict()
        _exchanges = returned_dict.pop("protocols")

        _chains = set()  # pyright: ignore [reportUnknownVariableType]  # should have only one element
        returned_dict["depositedExchanges"] = set()  # pyright: ignore [reportArgumentType]
        for cex_id, cex_deployments in _exchanges.items():  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType,reportAttributeAccessIssue]
            returned_dict["depositedExchanges"].add(cex_id)  # pyright: ignore [reportUnknownMemberType]
            _chains.add(cex_deployments[0]["chainId"])  # pyright: ignore [reportUnknownMemberType]

        try:
            assert len(_chains) == 1  # pyright: ignore [reportUnknownArgumentType]
        except AssertionError:
            raise AssertionError("'depositWallets' should only contain data on 1 chain")

        returned_dict["depositedExchanges"] = list(returned_dict["depositedExchanges"])  # pyright: ignore [reportUnknownArgumentType]
        returned_dict["chainId"] = _chains.pop()

        return returned_dict
