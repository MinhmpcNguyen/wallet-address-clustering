from typing import Dict


class Vertex:
    def __init__(self, chain_id, address):
        self._chain_id = chain_id
        self._address = address
        self._key = f"{self._chain_id}_{self._address}"
        self.last_transfer_at: int = 0

        # tags
        self.wallet: dict | None = None
        self.contract: bool = False

    def __eq__(self, other):
        return self._key == other._key

    def __hash__(self):
        return hash(self._key)

    def to_json_dict(self):
        return {
            "chainId": self._chain_id,
            "address": self._address,
            "lastTransferAt": self.last_transfer_at,
        }

    def get_tags(self) -> Dict:
        if self.wallet:
            return self.wallet
        else:
            return {"contract": self.contract}
