from models.graph.address_obj import Vertex
from typing import Set, Dict

ALLOWED_WALLET_TAGS = {'depositWallet', 'cexUser', 'hotWallet',
                       'dexUser', 'dexCreator'}


class Wallet(Vertex):
    def __init__(self, chain_id, address):
        super().__init__(chain_id, address)
        self._labels: Set[_WalletLabel] = set()

    def add_label(self, label: str):
        self._labels.add(_WalletLabel(label))

    def to_json_dict(self):
        returned_dict = super().to_json_dict()
        returned_dict['wallets']: Dict = {
            label.get_name(): True
            for label in self._labels
        }


class _WalletLabel:
    def __init__(self, name):
        if name not in ALLOWED_WALLET_TAGS:
            raise ValueError(f"Label must be in {ALLOWED_WALLET_TAGS}")
        else:
            self._name = name

    def __eq__(self, other):
        return self._name == other

    def __hash__(self):
        return hash(self._name)

    def get_name(self):
        return self._name
