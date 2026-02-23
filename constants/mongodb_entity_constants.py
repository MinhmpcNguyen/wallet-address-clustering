from constants.network_constants import Chains


class LPConstants:
    CHAIN_DEX_MAPPINGS: dict[str, list[str]] = {
        Chains.bsc: ["PancakePair"],
        Chains.fantom: ["Spooky LP"],
    }

    LP_NAME_ID_MAPPINGS: dict[str, str] = {
        "PancakePair": "pancakeswap",
        "Spooky LP": "spookyswap",
    }
