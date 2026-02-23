from typing_extensions import override

from constants.tag_constants import WalletTags


class Wallet:
    def __init__(self, address: str, last_updated_at: int, tag: str | None = None):
        self.address: str = address
        self.last_updated_at: int = last_updated_at
        self.tags: list[str] = list()
        if tag:
            self.add_tags(tag)

        self.protocols: dict[str, list[dict[str, str]]] = dict()

    def add_tags(self, new_tag: str):
        # if new_tag not in WalletTags.all_wallet_tags:
        if not hasattr(WalletTags, new_tag):
            print(f"{new_tag} not in supported wallet tags")
            return None
        if new_tag not in self.tags:
            self.tags.append(new_tag)

    def add_protocol(self, protocol_id: str, chain_id: str, address: str):
        protocol = dict(chain_id=chain_id, address=address)
        if protocol_id not in self.protocols:
            self.protocols[protocol_id] = list()
        self.protocols[protocol_id].append(protocol)

    def to_dict(self):
        returned_dict = {
            "address": self.address,
            "tags": self.tags,
            "lastUpdatedAt": self.last_updated_at,
            "protocols": {
                protocol_id: [
                    {"address": depl["address"], "chainId": depl["chain_id"]}
                    for depl in protocol_deployments
                ]
                for protocol_id, protocol_deployments in self.protocols.items()
            },
        }
        return returned_dict

    # @override
    # def __eq__(self, other):
    #     return self.address == other._address
    @override
    def __hash__(self):
        return hash(self.address)

    def not_empty(self) -> bool:
        if self.protocols:
            return True
        return False
