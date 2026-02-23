import os

POLYGON_AAVE_ADDRESS = "0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf"
ETHEREUM_AAVE_ADDRESS = "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9"

ETH_COMPOUND_ADDRESS = "0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b"

BSC_CREAM_ADDRESS = "0x589de0f0ccf905477646599bb3e5c622c84cc0ba"
ETH_CREAM_ADDRESS = "0x3d5bc3c8d13dcb8bf317092d84783c2697ae9258"

FTM_GEIST_ADDRESS = "0x9fad24f572045c7869117160a571b2e50b10d068"

BSC_TRAVA_ADDRESS = "0x75de5f7c91a89c16714017c7443eca20c7a8c295"
ETH_TRAVA_ADDRESS = "0xd61afaaa8a69ba541bc4db9c9b40d4142b43b9a4"
FTM_TRAVA_ADDRESS = "0xd98bb590bdfabf18c164056c185fbb6be5ee643f"

BSC_VALAS_ADDRESS = "0xe29a55a6aeff5c8b1beede5bcf2f0cb3af8f91f5"

BSC_VENUS_ADDRESS = "0xfd36e2c2a6789db23113685031d7f16329158384"


class Chains:
    bsc: str = "0x38"
    ethereum: str = "0x1"
    fantom: str = "0xfa"
    polygon: str = "0x89"
    arbitrum: str = "0xa4b1"
    optimism: str = "0xa"
    avalanche: str = "0xa86a"
    none_wrapped_token: list[str] = [arbitrum, fantom, optimism]
    all: list[str] = [bsc, ethereum, fantom, polygon, arbitrum, optimism, avalanche]
    mapping: dict[str, str] = {
        "bsc": bsc,
        "ethereum": ethereum,
        "fantom": fantom,
        "polygon": polygon,
        "arbitrum": arbitrum,
        "optimism": optimism,
        "avalanche": avalanche,
    }

    names: dict[str, str] = {
        bsc: "bsc",
        ethereum: "ethereum",
        fantom: "fantom",
        polygon: "polygon",
        arbitrum: "arbitrum",
        optimism: "optimism",
        avalanche: "avalanche",
    }

    abi_mapping: dict[str, str] = {
        bsc: "bep20_abi",
        ethereum: "erc20_abi",
        fantom: "erc20_abi",
        polygon: "erc20_abi",
        arbitrum: "erc20_abi",
        optimism: "erc20_abi",
        avalanche: "erc20_abi",
    }

    block_time: dict[str, int | float] = {
        bsc: 3,
        ethereum: 12,
        fantom: 1,
        polygon: 2,
        arbitrum: 0.3,
        avalanche: 2,
    }
    wrapped_native_token: dict[str, str] = {
        bsc: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
        ethereum: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        polygon: "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
        fantom: "0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83",
        arbitrum: "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        optimism: "0x4200000000000000000000000000000000000006",
        avalanche: "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
    }

    evm: dict[str, bool] = {
        ethereum: True,
        bsc: True,
        polygon: True,
        fantom: True,
        arbitrum: True,
        optimism: True,
        avalanche: True,
    }

    def get_abi_name(self, chain_id: str) -> str:
        """Map chain_id with the corresponding abi key
        e.g.: '0x38' -> 'bep20_abi' or '0x1' -> 'erc20_abi'
        """
        return self.abi_mapping.get(chain_id, "")


def get_chain_name(chain_id: str):
    return Chains.names.get(chain_id)


def get_chain_id(chain_name: str):
    return Chains.mapping.get(chain_name)


class Scans:
    mapping: dict[str, str] = {
        "bscscan": Chains.bsc,
        "etherscan": Chains.ethereum,
        "ftmscan": Chains.fantom,
        "polygonscan": Chains.polygon,
        "arbiscan": Chains.arbitrum,
        "optimistic": Chains.optimism,
        "snowtrace": Chains.avalanche,
    }

    all_base_urls: dict[str, list[str]] = {
        Chains.bsc: [
            "https://bscscan.com/token",
            "https://bscscan-com.translate.goog/token",
        ],
        Chains.polygon: [
            "https://polygonscan.com/token",
            "https://polygonscan-com.translate.goog/token",
        ],
        # Chains.ethereum: [
        #     'https://etherscan-io.translate.goog/token',
        # ],
        Chains.fantom: [
            "https://ftmscan.com/token",
            "https://ftmscan-com.translate.goog/token",
        ],
        Chains.arbitrum: [
            # 'https://arbiscan-io.translate.goog/token',
            "https://arbiscan.io/token"
        ],
        Chains.optimism: [
            # 'https://optimistic-etherscan-io.translate.goog/token',
            "https://optimistic.etherscan.io/token"
        ],
        Chains.avalanche: [
            # 'https://snowtrace-io.translate.goog/token',
            "https://snowtrace.io/token"
        ],
    }

    gg_translate_suffix: str = "?_x_tr_sl=vi&_x_tr_tl=en&_x_tr_hl=en&_x_tr_pto=wapp"


class Networks:
    bsc: str = "bsc"
    ethereum: str = "ethereum"
    fantom: str = "fantom"
    polygon: str = "polygon"
    arbitrum: str = "arbitrum"
    optimism: str = "optimism"
    avalanche: str = "avalanche"

    providers: dict[str, str] = {
        bsc: os.getenv("BSC_PROVIDER_URI", "https://bsc-dataseed1.binance.org/"),
        ethereum: os.getenv(
            "ETHEREUM_PROVIDER_URI",
            "https://rpc.ankr.com/eth/e6ef0039249ca57e768cf547b4e0ab5afb5ccb2d6a7251aa8df36b2c6022806d",
        ),
        fantom: os.getenv("FANTOM_PROVIDER_URI", "https://rpc.ftm.tools/"),
        polygon: os.getenv("POLYGON_PROVIDER_URI", "https://polygon-rpc.com"),
        arbitrum: os.getenv(
            "ARBITRUM_PROVIDER_URI",
            "https://endpoints.omniatech.io/v1/arbitrum/one/public",
        ),
        optimism: os.getenv("OPTIMISM_PROVIDER_URI", "https://rpc.ankr.com/optimism"),
        avalanche: os.getenv(
            "AVALANCHE_PROVIDER_URI", "https://rpc.ankr.com/avalanche"
        ),
    }

    archive_node: dict[str, str] = {
        bsc: os.getenv("BSC_PROVIDER_ARCHIVE_URI", "https://rpc.ankr.com/bsc"),
        ethereum: os.getenv(
            "ETHEREUM_PROVIDER_ARCHIVE_URI", "https://rpc.ankr.com/eth"
        ),
        fantom: os.getenv("FANTOM_PROVIDER_ARCHIVE_URI", "https://rpc.ankr.com/fantom"),
        polygon: os.getenv(
            "POLYGON_PROVIDER_ARCHIVE_URI", "https://rpc.ankr.com/polygon"
        ),
        arbitrum: os.getenv(
            "ARBITRUM_PROVIDER_ARCHIVE_URI", "https://rpc.ankr.com/arbitrum"
        ),
        optimism: os.getenv(
            "OPTIMISM_PROVIDER_ARCHIVE_URI", "https://rpc.ankr.com/optimism"
        ),
        avalanche: os.getenv(
            "AVALANCHE_PROVIDER_ARCHIVE_URI", "https://rpc.ankr.com/avalanche"
        ),
    }


NATIVE_TOKEN = "0x0000000000000000000000000000000000000000"

NATIVE_TOKENS = {
    Chains.bsc: "0x0000000000000000000000000000000000000000",
    Chains.ethereum: "0x0000000000000000000000000000000000000000",
    Chains.fantom: "0x0000000000000000000000000000000000000000",
    Chains.polygon: "0x0000000000000000000000000000000000000000",
    Chains.arbitrum: "0x0000000000000000000000000000000000000000",
    Chains.optimism: "0x0000000000000000000000000000000000000000",
    Chains.avalanche: "0x0000000000000000000000000000000000000000",
}

NATIVE_TOKENS_DECIMALS = {
    Chains.bsc: 10**18,
    Chains.ethereum: 10**18,
    Chains.fantom: 10**18,
    Chains.polygon: 10**18,
    Chains.arbitrum: 10**18,
    Chains.optimism: 10**18,
    Chains.avalanche: 10**18,
}
