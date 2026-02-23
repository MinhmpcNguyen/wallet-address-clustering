import os
import re
import tempfile
from typing import Any, Dict, List, Tuple

import numpy as np
import numpy.typing as npt
from gensim.models import Word2Vec
from sklearn.metrics.pairwise import cosine_similarity

from diff2vec.diffusion_2_vec import (
    learn_pooled_embeddings,
    run_parallel_feature_creation,
)
from utils.logger_utils import get_logger

logger = get_logger("EmbeddingUtils")

_EMB_DIM = 24  # must match learn_pooled_embeddings(vector_size=24)


class EmbeddingUtils:
    # ---------------- helpers ----------------
    @staticmethod
    def resolve_nodes(row: dict[str, Any]) -> list[str]:
        """Public helper: normalize edge source and resolve ordered unique node list."""
        edges_like = EmbeddingUtils._normalize_edges_source(row)
        nodes = EmbeddingUtils._resolve_nodes(row, edges_like)
        logger.debug(
            "resolve_nodes: got %d nodes (has_edges=%s)",
            len(nodes),
            bool(edges_like) and not isinstance(edges_like, (str, type(None))),
        )
        return nodes

    @staticmethod
    def _normalize_edges_source(row: Dict[str, Any]) -> Any:
        """
        Prefer row['edges']; if missing, fall back to row['vertices'] when it actually
        contains a list of edges.
        Returns either:
          - a string path to an edge list file, or
          - a list of edge dicts, e.g. [{'from':'A','to':'B'}] or [{'_from':'A','_to':'B'}]
        """
        edges_like = row.get("edges")
        if edges_like is None:
            edges_like = row.get(
                "vertices"
            )  # older pipelines stored edges under 'vertices'
        logger.debug("_normalize_edges_source: type=%s", type(edges_like).__name__)
        return edges_like

    @staticmethod
    def _resolve_nodes(row: Dict[str, Any], edges_like: Any) -> List[str]:
        """
        Build node list:
        - If row['vertices'] is a list[str], use it (deduplicated, original order kept).
        - Otherwise infer nodes from edge list (edges_like).
        """
        verts = row.get("vertices")
        if isinstance(verts, list) and verts and isinstance(verts[0], str):
            seen, out = set(), []
            for v in verts:
                if v not in seen:
                    seen.add(v)
                    out.append(v)
            logger.debug("_resolve_nodes: used vertices list, unique=%d", len(out))
            return out

        # infer from edges
        addrs = set()
        if isinstance(edges_like, list):
            for e in edges_like:
                if not isinstance(e, dict):
                    continue
                u = e.get("_from") or e.get("from")
                v = e.get("_to") or e.get("to")
                if isinstance(u, str) and u:
                    addrs.add(u)
                if isinstance(v, str) and v:
                    addrs.add(v)
        nodes = sorted(addrs)
        logger.debug("_resolve_nodes: inferred from edges, unique=%d", len(nodes))
        return nodes

    @staticmethod
    def _edges_to_path(edges_like: Any) -> Tuple[str, bool]:
        """
        Return (path, is_temp):
          - if edges_like is a string -> (edges_like, False)
          - if edges_like is a list of edges -> write to a temporary file -> (path, True)
        """
        if isinstance(edges_like, str):
            logger.debug("_edges_to_path: using provided path '%s'", edges_like)
            return edges_like, False

        if isinstance(edges_like, list) and edges_like:
            fd, path = tempfile.mkstemp(prefix="edgelist_", suffix=".txt")
            os.close(fd)
            seen = set()
            with open(path, "w", encoding="utf-8") as f:
                for e in edges_like:
                    if not isinstance(e, dict):
                        continue
                    u = e.get("_from") or e.get("from")
                    v = e.get("_to") or e.get("to")
                    if isinstance(u, str) and isinstance(v, str) and u and v:
                        key = (u, v)
                        if key in seen:
                            continue
                        seen.add(key)
                        f.write(f"{u} {v}\n")
            logger.debug(
                "_edges_to_path: wrote %d unique edges to temp '%s'", len(seen), path
            )
            return path, True

        logger.error(
            "_edges_to_path: invalid edges_like; must be path string or list of edge dicts"
        )
        raise ValueError(
            "'edges' or 'vertices'(as edges) must be a path or list of {from,to}/({_from,_to})."
        )

    # ---------------- public APIs ----------------
    @staticmethod
    def get_diff2vec_embedding(
        row: Dict[str, Any],
        *,
        vertex_set_card: int | None = None,
        replicates: int = 4,
        workers: int = 4,
    ) -> List[npt.NDArray[np.float_]]:
        """
        Generate Diff2Vec embeddings for one subgraph document.
        Supported schemas:
          1) row = {'edges': <path or list of edges>, 'vertices': list[str] (optional)}
          2) row = {'vertices': list of edges [{_from/_to}...]} (no 'edges' key)
        Returns a list of vectors ordered according to resolved nodes.
        """
        edges_like = EmbeddingUtils._normalize_edges_source(row)
        if edges_like is None:
            logger.error(
                "get_diff2vec_embedding: missing 'edges' and no usable 'vertices' as edges"
            )
            raise ValueError("Missing 'edges' and no usable 'vertices' as edges.")

        nodes = EmbeddingUtils._resolve_nodes(row, edges_like)
        if not nodes:
            logger.error(
                "get_diff2vec_embedding: cannot resolve node list from 'vertices' or edges"
            )
            raise ValueError("Cannot resolve node list from 'vertices' or edges.")

        if vertex_set_card is None:
            # backward compatible: number of diffusions = min(|V|, 1024) but >= 1
            vertex_set_card = max(1, min(len(nodes), 1024))
        logger.info(
            "get_diff2vec_embedding: nodes=%d, vertex_set_card=%d, replicates=%d, workers=%d",
            len(nodes),
            vertex_set_card,
            replicates,
            workers,
        )

        path, is_temp = EmbeddingUtils._edges_to_path(edges_like)
        try:
            walks, counts = run_parallel_feature_creation(
                edge_list_path=path,
                vertex_set_card=vertex_set_card,
                replicates=replicates,
                workers=workers,
            )
            logger.debug(
                "diff2vec: generated walks=%d, distinct_vertices=%d",
                len(walks) if hasattr(walks, "__len__") else -1,
                len(counts) if hasattr(counts, "__len__") else -1,
            )
            model: Word2Vec = learn_pooled_embeddings(walks, counts)

            zero = np.zeros((_EMB_DIM,), dtype=np.float32)
            vecs: List[npt.NDArray[np.float_]] = [
                (model.wv[v] if v in model.wv else zero) for v in nodes
            ]
            oov = sum(1 for v in nodes if v not in model.wv)
            if oov:
                logger.debug(
                    "diff2vec: %d/%d nodes were OOV -> zero vectors", oov, len(nodes)
                )
            return vecs
        finally:
            if is_temp and os.path.exists(path):
                try:
                    os.remove(path)
                    logger.debug("Removed temp edgelist: %s", path)
                except Exception:
                    logger.warning(
                        "Failed to remove temp file: %s", path, exc_info=True
                    )

    @staticmethod
    def diff_cosine(row: Dict[str, Any]) -> float:
        """Cosine similarity between two diff2vec embeddings present in row."""
        v1 = np.asarray(row["X_Diff2VecEmbedding"], dtype=np.float32).reshape(1, -1)
        v2 = np.asarray(row["SubX_Diff2VecEmbedding"], dtype=np.float32).reshape(1, -1)
        if v1.size == 0 or v2.size == 0:
            logger.debug("diff_cosine: empty vectors encountered -> returning 0.0")
            return 0.0
        sim = float(cosine_similarity(v1, v2)[0, 0])
        logger.debug("diff_cosine: similarity=%.6f", sim)
        return sim

    @staticmethod
    def get_embedding_list(s: str) -> List[float]:
        """Extract list of floats from a string like '[0.1, 0.2, -0.3]'."""
        nums = re.findall(r"-?\d+\.\d+", s)
        if not nums:
            logger.error("get_embedding_list: no valid numbers found in input string")
            raise ValueError("No valid numbers found.")
        out = [float(x) for x in nums]
        logger.debug("get_embedding_list: parsed %d numbers", len(out))
        return out
