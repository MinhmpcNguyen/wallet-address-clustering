"""Helper functions."""

import numpy as np
from gensim.models.doc2vec import TaggedDocument
from texttable import Texttable
from tqdm import tqdm


def generation_tab_printer(read_times, generation_times):
    """
    Function to print the time logs in a nice tabular format.
    :param read_times: List of reading times.
    :param generation_times: List of generation times.
    """
    t = Texttable()
    t.add_rows(
        [
            ["Metric", "Value"],
            ["Mean graph read time:", np.mean(read_times)],
            ["Standard deviation of read time.", np.std(read_times)],
        ]
    )
    print(t.draw())
    t = Texttable()
    t.add_rows(
        [
            ["Metric", "Value"],
            ["Mean sequence generation time:", np.mean(generation_times)],
            ["Standard deviation of generation time.", np.std(generation_times)],
        ]
    )
    print(t.draw())


def result_processing(results):
    """
    Function to separate the sequences from time measurements and process them.
    :param results: List of 3-length tuples including the sequences and results.
    :return walk_results: List of random walks.
    :return counts: Number of nodes.
    """
    walk_results = [res[0] for res in results]
    read_time_results = [res[1] for res in results]
    generation_time_results = [res[2] for res in results]
    counts = [res[3] for res in results]
    generation_tab_printer(read_time_results, generation_time_results)
    walk_results = [walk for walks in walk_results for walk in walks]
    return walk_results, counts


def process_non_pooled_model_data(walks, counts):
    """
    Function to extract proximity statistics.
    :param walks: Diffusion lists.
    :param counts: Number of nodes.
    :return docs: Processed walks.
    """
    print("Run feature extraction across windows.")
    features = {str(node): [] for node in range(counts)}
    for walk in tqdm(walks):
        for i in range(len(walk) - 10):
            for j in range(1, 10 + 1):
                features[walk[i]].append(["+" + str(j) + "_" + walk[i + j]])
                features[walk[i + j]].append(["_" + str(j) + "_" + walk[i]])

    docs = [
        TaggedDocument(words=[x[0] for x in v], tags=[str(k)])
        for k, v in features.items()
    ]
    return docs
