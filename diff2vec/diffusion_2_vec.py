"""Diff2Vec model."""

# import logging
# import numpy.distutils.system_info as sysinfo
from gensim.models import Word2Vec
from joblib import Parallel, delayed
from tqdm import tqdm

from diff2vec.helper import result_processing
from diff2vec.subgraph_components import SubGraphComponents

# from src.helper import process_non_pooled_model_data, argument_printer


# sysinfo.get_info("atlas")
# logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO)


def create_features(seeding: int, edge_list_path: str, vertex_set_cardinality: int):
    """
    Creating a single feature for every node.
    :param seeding: Random seed.
    :param edge_list_path:  Path to edge list csv.
    :param vertex_set_cardinality: Number of diffusions per node.
    :return: Sequences and measurements.
    """
    sub_graphs = SubGraphComponents(edge_list_path, seeding, vertex_set_cardinality)
    return (
        sub_graphs.paths,
        sub_graphs.read_time,
        sub_graphs.generation_time,
        sub_graphs.counts,
    )


def run_parallel_feature_creation(edge_list_path, vertex_set_card, replicates, workers):
    """
    Creating linear node sequences for every node multiple times in a parallel fashion
    :param edge_list_path: Path to edge list csv.
    :param vertex_set_card: Number of diffusions per node.
    :param replicates: Number of unique nodes per diffusion.
    :param workers: Number of cores used.
    :return walk_results: List of 3-length tuples with sequences and performance measurements.
    :return counts: Number of nodes.
    """
    results = Parallel(n_jobs=workers)(
        delayed(create_features)(i, edge_list_path, vertex_set_card)
        for i in tqdm(range(replicates))
    )
    walk_results, counts = result_processing(results)
    return walk_results, counts


def learn_pooled_embeddings(walks: list[list[str]], counts: int):
    """
    Method to learn an embedding given the sequences and arguments.
    :param walks: Linear vertex sequences.
    :param counts: Number of nodes.
    """
    model = Word2Vec(
        walks,
        vector_size=24,
        window=10,
        min_count=1,
        sg=1,
        workers=4,
        epochs=1,
        alpha=0.025,
    )

    return model
