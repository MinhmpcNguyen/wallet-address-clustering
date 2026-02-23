from constants.network_constants import Chains


class ArangoDBPrefix:
    mapping: dict[str, str] = {
        Chains.bsc: "bsc",
        Chains.ethereum: "ethereum",
        Chains.fantom: "ftm",
        Chains.polygon: "polygon",
    }
    reversed_mapping: dict[str, str] = {v: k for k, v in mapping.items()}


class ArangoDBCollectionsByChain:
    def __init__(self, prefix: str):
        self.configs: str = f"{prefix}_configs"
        self.transfers: str = f"{prefix}_transfers"
        self.addresses: str = f"{prefix}_addresses"


class ArangoDBGraphsByChain:
    def __init__(self, prefix: str):
        self.transfers_graph: str = f"{prefix}_transfers_graph"


class KnowledgeGraphModelByChain:
    def __init__(self, prefix: str):
        self.edge_definitions: list[dict[str, str | list[str]]] = [
            {
                "edge_collection": f"{prefix}_{ArangoDBCollections.transfers}",
                "from_vertex_collections": [
                    f"{prefix}_{ArangoDBCollections.addresses}",
                ],
                "to_vertex_collections": [
                    f"{prefix}_{ArangoDBCollections.addresses}",
                ],
            }
        ]


class ArangoDBCollections:
    configs: str = "configs"
    transfers: str = "transfers"
    addresses: str = "addresses"


class ArangoDBGraphs:
    transfers_graph: str = "transfers_graph"


class KnowledgeGraphModel:
    edge_definitions: list[dict[str, str | list[str]]] = [
        {
            "edge_collection": ArangoDBCollections.transfers,
            "from_vertex_collections": [
                ArangoDBCollections.addresses,
            ],
            "to_vertex_collections": [
                ArangoDBCollections.addresses,
            ],
        }
    ]


class ArangoDBKeys:
    pass


class ArangoDBIndex:
    pass
