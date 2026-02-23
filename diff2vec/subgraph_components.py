"""Subgraph components module."""

import random
import time

import networkx as nx

from diff2vec.diffusion_trees import EulerianDiffuser


def get_graph(row):
    edges_lst = row
    G = nx.DiGraph()
    for edge in edges_lst:
        G.add_edge(edge["from"], edge["to"])
    # Create an empty MultiGraph
    multi_G = nx.MultiGraph()

    # Iterate through the directed edges and add them as undirected edges with attributes
    for u, v, attrs in G.edges(data=True):
        multi_G.add_edge(u, v, **attrs)
    return multi_G


class SubGraphComponents:
    def __init__(self, edge_list_path, seeding, vertex_set_cardinality):
        self.seed = seeding
        self.vertex_set_cardinality = vertex_set_cardinality
        self.read_start_time = time.time()
        self.graph = get_graph(edge_list_path)
        self.counts = len(self.graph.nodes()) + 1
        self.separate_subcomponents()
        self.single_feature_generation_run()

    def separate_subcomponents(self):
        comps = [self.graph.subgraph(c) for c in nx.connected_components(self.graph)]
        self.graph = sorted(comps, key=len, reverse=True)
        self.read_time = time.time() - self.read_start_time

    def single_feature_generation_run(self):
        random.seed(self.seed)
        self.generation_start_time = time.time()
        self.paths = {}
        for sub_graph in self.graph:
            current_cardinality = len(sub_graph.nodes())
            if current_cardinality < self.vertex_set_cardinality:
                self.vertex_set_cardinality = current_cardinality
            diffuser = EulerianDiffuser(sub_graph, self.vertex_set_cardinality)
            self.paths.update(diffuser.diffusions)
        self.paths = [v for k, v in self.paths.items()]
        self.generation_time = time.time() - self.generation_start_time
