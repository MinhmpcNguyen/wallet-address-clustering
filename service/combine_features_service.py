from __future__ import annotations

import random
import time
from typing import Any, cast

from pymongo import UpdateOne
from pymongo import errors as pymongo_errors
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.database import Database

from hooks.errors import (
    ComputationError,
    DatabaseError,
    DatabaseWriteError,
    DataValidationError,
    DuplicateKeyError,
    QueryError,
    SchemaMismatchError,
)
from schemas.combine_feat_schema import TrainPairDoc
from schemas.deposit_schema import DepositReusePairDoc
from schemas.from_to_schema import FromToDoc
from utils.embedding_utils import EmbeddingUtils
from utils.logger_utils import get_logger

logger = get_logger("CombineFeatures(Create_dataset)")


def _to_float_maybe(x: str | list[int]):
    """
    Try to convert a value into float if possible.
    - int/float -> float
    - numeric string -> float (handles commas)
    - else -> None
    """
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, list):
        return [float(i) for i in x]
    if x:
        try:
            return float(x.replace(",", ""))
        except ValueError:
            return None
    return None


def _copy_token_features(
    src: dict[str, str | list[int]],
    out: TrainPairDoc,
    *,
    src_prefix: str,  # e.g. "From_" or "To_"
    dst_prefix: str,  # e.g. "X_" or "SubX_"
) -> set[str]:
    """
    Copy token-related features from src into out.

    Example:
        If src has {"From_0x...": 123.0},
        and dst_prefix="X_", src_prefix="From_",
        then out will include {"X_From_0x...": 123.0}.

    Notes:
    - Only copies keys that start with src_prefix.
    - Skips 'time' arrays (e.g. From_time).
    - Converts numeric-like values to float.
    - Returns the set of keys copied (useful for debugging).
    """
    try:
        written: set[str] = set()
        for k, v in src.items():
            # No need to check isinstance(k, str) as keys are guaranteed to be strings
            if not k.startswith(src_prefix):
                continue
            if k == f"{src_prefix}time":  # skip histogram
                continue

            fv = _to_float_maybe(v)
            if fv is None:
                continue

            out[f"{dst_prefix}{k}"] = fv  # keep both prefixes
            written.add(k)
        return written
    except Exception as exc:
        raise (
            SchemaMismatchError("Failed to copy token features")
            .with_context(prefix=src_prefix, dst_prefix=dst_prefix)
            .with_cause(exc)
        )


def _sum_24(a: list[int] | None, b: list[int] | None) -> list[int]:
    """Sum two 24-bin histograms. Missing -> 24 zeros; truncate/pad to 24."""
    try:
        if not isinstance(a, list):
            a = [0] * 24
        if not isinstance(b, list):
            b = [0] * 24
        la, lb = len(a), len(b)
        if la != 24 or lb != 24:
            logger.debug(
                "_sum_24: non-24 lengths detected (la=%d, lb=%d), padding/truncating",
                la,
                lb,
            )
        if la != 24:
            a = (a + [0] * 24)[:24]
        if lb != 24:
            b = (b + [0] * 24)[:24]
        return [int(a[i]) + int(b[i]) for i in range(24)]
    except Exception as exc:
        raise DataValidationError("Invalid 24-bin histogram input").with_cause(exc)


SG_FALLBACK = "other_token"


class ProcessTrainingDatasetMongo:
    """
    End-to-end pipeline: Mongo -> Mongo, no pandas/CSV.
    """

    def __init__(
        self,
        db: Database[Any],
        *,
        # INPUT collections
        from_col_name: str,
        to_col_name: str,
        embedding_col_name: str,  # docs: { subgraphId, address, embedding: [float,...] }
        pairs_col_name: str,  # docs: { X_address, SubX_address, ... }
        contracts_col_name: str
        | None = None,  # optional: { address, IsContract: bool }
        # OUTPUT collections
        out_train_col_name: str,
        out_test_col_name: str,
        # options
        compute_embedding_similarity: bool = False,
        train_ratio: float = 0.9,
        chain_id: str | None = None,
        balance_train_by_label: bool = True,
        random_seed: int | None = 42,
    ):
        self.db: Database[Any] = db
        try:
            self.col_from: Collection[FromToDoc] = db[from_col_name]
            self.col_to: Collection[FromToDoc] = db[to_col_name]
            self.col_emb: Collection[Any] = db[embedding_col_name]
            self.col_pairs: Collection[DepositReusePairDoc] = db[pairs_col_name]
            self.col_contracts: Collection[Any] | None = (
                db[contracts_col_name] if contracts_col_name else None
            )
            self.col_train: Collection[TrainPairDoc] = db[out_train_col_name]
            self.col_test: Collection[TrainPairDoc] = db[out_test_col_name]
        except Exception as exc:
            raise QueryError("Failed to resolve Mongo collections").with_cause(exc)

        self.compute_embedding_similarity: bool = compute_embedding_similarity
        self.train_ratio: float = max(0.0, min(1.0, train_ratio))
        self.chain_id: str | None = chain_id
        self.balance_train_by_label: bool = balance_train_by_label
        self.random_seed: int | None = random_seed

        # Suggested indexes (best-effort)
        try:
            _ = self.col_emb.create_index(
                [("subgraphId", 1), ("address", 1)], name="emb_sg_addr"
            )
            _ = self.col_pairs.create_index(
                [("X_address", 1), ("SubX_address", 1)], name="pair_xy"
            )
            _ = self.col_train.create_index([("X_address", 1)], name="train_x")
            _ = self.col_test.create_index([("X_address", 1)], name="test_x")
        except Exception as exc:
            logger.debug("Index creation skipped/failed: %s", exc)

    def run(self) -> None:
        logger.info("Start building training dataset from Mongo…")
        t0 = time.time()
        try:
            # 1) Read FROM and TO -> map by (_id,address)
            from_map, _ = self._load_side(self.col_from, prefix="From_")
            to_map, _ = self._load_side(self.col_to, prefix="To_")

            # 2) Merge from/to -> records by (_id,address)
            merged = self._merge_from_to(from_map, to_map)

            # 3) Join embeddings by (subgraphId=_id,address)
            if self.compute_embedding_similarity:
                emb_map = self._load_embeddings()
                _ = self._attach_embeddings(merged, emb_map)  # pyright: ignore [reportUnknownMemberType]

            # 4) (optional) filter contracts
            if self.col_contracts is not None:
                _ = self._filter_contracts_inplace(merged)  # pyright: ignore [reportUnknownMemberType]

            # 5) Build pair features + labels from pairs collection
            label_set = self._load_pair_label_set()
            pair_records = self._build_pairs_and_label(merged, label_set)

            # 6) Split train/test by X_address
            if self.balance_train_by_label:
                train_docs, test_docs = self._balanced_train_test_by_label(
                    pair_records, self.train_ratio, self.random_seed
                )
            else:
                train_docs, test_docs = self._split_train_test(pair_records)

            # 7) Write straight to Mongo
            self._write_output(self.col_train, train_docs)
            self._write_output(self.col_test, test_docs)

            elapsed = time.time() - t0
            logger.info(
                "Done. Train docs: %d / Test docs: %d (chainId=%s) in %.2fs",
                len(train_docs),
                len(test_docs),
                self.chain_id or "n/a",
                elapsed,
            )
        except (
            DatabaseError,
            DataValidationError,
            SchemaMismatchError,
            ComputationError,
        ) as exc:
            # Known mapped errors: bubble up
            logger.error("Process failed with mapped error: %s", exc)
            raise
        except Exception as exc:
            # Unknown errors: wrap
            logger.exception("Unexpected failure in ProcessTrainingDatasetMongo.run()")
            raise DatabaseError("Pipeline failed unexpectedly").with_cause(exc)

    # ------------------------ LOAD & MERGE ------------------------

    def _load_side(
        self, col: Collection[FromToDoc], *, prefix: str
    ) -> tuple[dict[tuple[str, str], dict[str, str | list[int]]], set[str]]:
        logger.info("Loading side: %s", col.name)
        out: dict[tuple[str, str], dict[str, str | list[int]]] = {}
        idset: set[str] = set()

        cnt = 0
        try:
            cursor = col.find({})
        except Exception as exc:
            raise (
                QueryError("Mongo find() failed")
                .with_context(collection=col.name)
                .with_cause(exc)
            )

        try:
            for d in cursor:
                sg: str = d.get("subgraphId", d.get("_id"))

                addr = d.get("address")
                if not addr:
                    logger.debug("_load_side skip doc without address: %s", d)
                    continue

                idset.add(sg)
                key = (sg, addr)
                doc = out.setdefault(key, {"_id": sg, "address": addr})
                doc[f"{prefix}time"] = d.get("time") or [0] * 24

                for k, v in d.items():
                    if k in ("_id", "subgraphId", "address", "time"):
                        continue
                    # if isinstance(v, (str, list)) and (
                    #     not isinstance(v, list) or all(isinstance(i, int) for i in v)
                    # ):
                    doc[f"{prefix}{k}"] = v  # pyright: ignore [reportArgumentType]
                cnt += 1
        except Exception as exc:
            raise (
                DataValidationError("Failed while loading side collection")
                .with_context(collection=col.name, prefix=prefix)
                .with_cause(exc)
            )

        logger.info(
            "Loaded %d records from %s (unique keys=%d)", cnt, col.name, len(out)
        )
        return out, idset

    def _merge_from_to(
        self,
        from_map: dict[tuple[str, str], dict[str, str | list[int]]],
        to_map: dict[tuple[str, str], dict[str, str | list[int]]],
    ) -> dict[tuple[str, str], dict[str, str | list[int]]]:
        logger.info("Merging FROM and TO …")
        try:
            keys = set(from_map.keys()) | set(to_map.keys())
            merged: dict[tuple[str, str], dict[str, str | list[int]]] = {}

            for key in keys:
                base: dict[str, str | list[int]] = {"_id": key[0], "address": key[1]}
                fdoc: dict[str, str | list[int]] = from_map.get(key, {})
                tdoc: dict[str, str | list[int]] = to_map.get(key, {})

                for k, v in fdoc.items():
                    if k not in ("_id", "address"):
                        base[k] = v
                for k, v in tdoc.items():
                    if k not in ("_id", "address"):
                        base[k] = v

                # Keep explicit list[int] typing with safe fallbacks
                ftime: list[int] = cast(list[int], base.get("From_time") or [])
                ttime: list[int] = cast(list[int], base.get("To_time") or [])

                base["Time"] = _sum_24(ftime, ttime)
                merged[key] = base
        except Exception as exc:
            raise SchemaMismatchError("Failed to merge FROM/TO maps").with_cause(exc)

        logger.info("Merged records: %d", len(merged))
        return merged

    # ------------------------ EMBEDDINGS ------------------------

    def _load_embeddings(self) -> dict[tuple[Any, str], list[float]]:
        logger.info("Loading embeddings from %s", self.col_emb.name)
        out: dict[tuple[Any, str], list[float]] = {}
        proj = {"_id": 0, "subgraphId": 1, "address": 1, "embedding": 1}
        cnt = 0
        try:
            cursor = self.col_emb.find({}, proj)
        except Exception as exc:
            raise QueryError("Mongo find() failed for embeddings").with_cause(exc)

        try:
            for d in cursor:
                sg = d.get("subgraphId")
                if sg is None:
                    sg = SG_FALLBACK
                addr = d.get("address")
                vec = d.get("embedding")
                if not addr or not isinstance(vec, list):
                    logger.debug(
                        "_load_embeddings skip invalid row: addr=%s vecType=%s",
                        addr,
                        type(vec),  # pyright: ignore [reportUnknownArgumentType]
                    )
                    continue
                out[(sg, addr)] = vec
                cnt += 1
        except Exception as exc:
            raise DataValidationError("Invalid embedding records").with_cause(exc)

        logger.info("Loaded embeddings: %d", cnt)
        return out

    def _attach_embeddings(
        self,
        merged: dict[tuple[Any, str], dict],  # pyright: ignore [reportUnknownParameterType,reportMissingTypeArgument]
        emb_map: dict[tuple[Any, str], list[float]],
    ) -> None:
        try:
            attach_cnt = 0
            for key, doc in merged.items():  # pyright: ignore [reportUnknownVariableType]
                emb = emb_map.get(key)
                if emb is not None:
                    doc["Diff2VecEmbedding"] = emb
                    attach_cnt += 1
            logger.info("Attached embeddings to %d/%d nodes", attach_cnt, len(merged))  # pyright: ignore [reportUnknownArgumentType]
        except Exception as exc:
            raise SchemaMismatchError("Failed to attach embeddings").with_cause(exc)

    # ------------------------ CONTRACT FILTER ------------------------

    def _filter_contracts_inplace(self, merged: dict[tuple[Any, str], dict]) -> None:  # pyright: ignore [reportUnknownParameterType,reportMissingTypeArgument]
        logger.info("Filtering contracts …")
        try:
            contract_set: set[str] = set()
            proj = {"_id": 0, "address": 1, "IsContract": 1}
            if self.col_contracts is not None:
                for d in self.col_contracts.find({"IsContract": True}, proj):
                    a = d.get("address")
                    if a:
                        contract_set.add(a)

            drop_keys = [k for k in merged if k[1] in contract_set]
            for k in drop_keys:
                _ = merged.pop(k, None)  # pyright: ignore [reportUnknownMemberType,reportUnknownVariableType]
            logger.info(
                "Removed %d contract addresses (remaining=%d)",
                len(drop_keys),
                len(merged),  # pyright: ignore [reportUnknownArgumentType]
            )
        except Exception as exc:
            raise QueryError("Failed while filtering contracts").with_cause(exc)

    # ------------------------ PAIRS & LABELS ------------------------

    def _load_pair_label_set(self) -> set[tuple[str, str]]:
        logger.info("Loading pair labels from %s", self.col_pairs.name)
        try:
            s: set[tuple[str, str]] = set()
            proj = {"_id": 0, "X_address": 1, "SubX_address": 1}
            cnt = 0
            for d in self.col_pairs.find({}, proj):
                xa = d.get("X_address")
                ya = d.get("SubX_address")
                if xa and ya:
                    s.add((xa, ya))
                    cnt += 1
            logger.info("Loaded %d labeled pairs", cnt)
            return s
        except Exception as exc:
            raise QueryError("Failed to load pair label set").with_cause(exc)

    def _build_pairs_and_label(
        self,
        merged: dict[tuple[str, str], dict[str, str | list[int]]],
        label_set: set[tuple[str, str]],
    ) -> list[TrainPairDoc]:
        logger.info("Building pair features + labels …")
        try:
            by_sg: dict[str, list[dict[str, str | list[int]]]] = {}
            for (_, _addr), docs in merged.items():
                sg: str = str(docs.get("_id", SG_FALLBACK))
                by_sg.setdefault(sg, []).append(docs)
        except Exception as exc:
            raise SchemaMismatchError("Failed to group by subgraph").with_cause(exc)

        out_docs: list[TrainPairDoc] = []
        now = int(time.time())

        try:
            for sg, rows in by_sg.items():
                sg_str = str(sg)
                parts = sg_str.split("_")
                center_addr = parts[1] if len(parts) >= 2 else None

                X_rows: list[dict[str, str | list[int]]] = []
                SubX_rows: list[dict[str, str | list[int]]] = []
                for r in rows:
                    if center_addr is not None and r.get("address") == center_addr:
                        X_rows.append(r)
                    else:
                        SubX_rows.append(r)

                if not X_rows or not SubX_rows:
                    if rows:
                        X_rows = [rows[0]]
                        SubX_rows = rows[1:]
                    if not X_rows or not SubX_rows:
                        logger.debug(
                            "_build_pairs_and_label: skip sg=%s due to insufficient rows",
                            sg,
                        )
                        continue

                x: dict[str, str | list[int]] = X_rows[0]
                X_emb = x.get("Diff2VecEmbedding")

                for sub in SubX_rows:
                    xa = str(x.get("address"))
                    ya = str(sub.get("address"))
                    if not xa or not ya:
                        logger.debug(
                            "_build_pairs_and_label: missing address in sg=%s", sg
                        )
                        continue

                    doc = TrainPairDoc(
                        _id=sg,
                        X_address=xa,
                        SubX_address=ya,
                        X_Time=cast(list[int], x.get("Time", [0] * 24))
                        if isinstance(x.get("Time", [0] * 24), list)
                        else [0] * 24,
                        SubX_Time=cast(list[int], sub.get("Time", [0] * 24))
                        if isinstance(sub.get("Time", [0] * 24), list)
                        else [0] * 24,
                    )
                    _ = _copy_token_features(
                        x, doc, src_prefix="From_", dst_prefix="X_"
                    )
                    _ = _copy_token_features(
                        sub, doc, src_prefix="From_", dst_prefix="SubX_"
                    )
                    _ = _copy_token_features(x, doc, src_prefix="To_", dst_prefix="X_")
                    _ = _copy_token_features(
                        sub, doc, src_prefix="To_", dst_prefix="SubX_"
                    )

                    if self.compute_embedding_similarity:
                        sub_emb = sub.get("Diff2VecEmbedding")
                        if isinstance(X_emb, list) and isinstance(sub_emb, list):
                            try:
                                sim = EmbeddingUtils.diff_cosine(
                                    {
                                        "X_Diff2VecEmbedding": X_emb,
                                        "SubX_Diff2VecEmbedding": sub_emb,
                                    }
                                )
                            except Exception as exc:
                                logger.debug(
                                    "diff_cosine failed for sg=%s: %s", sg, exc
                                )
                                # Map to computation error but continue with default 0.0
                                sim = 0.0
                            doc["Diff2_Vec_Simi"] = float(sim)

                    doc["Label"] = (xa, ya) in label_set
                    if self.chain_id:
                        doc["chainId"] = self.chain_id
                    doc["updatedAt"] = now
                    _ = out_docs.append(doc)
        except ComputationError:
            # If you raise ComputationError inside, bubble up
            raise
        except Exception as exc:
            raise SchemaMismatchError(
                "Failed to build pair features/labels"
            ).with_cause(exc)

        logger.info("Built %d pair docs", len(out_docs))
        return out_docs

    # ------------------------ SPLIT & WRITE ------------------------

    def _split_train_test(
        self, docs: list[TrainPairDoc]
    ) -> tuple[
        list[TrainPairDoc],
        list[TrainPairDoc],
    ]:
        try:
            if not docs:
                return [], []
            addrs = sorted(
                {
                    str(d.get("X_address"))
                    for d in docs
                    if isinstance(d.get("X_address"), (str, int))
                }
            )
            if not addrs:
                k = int(len(docs) * self.train_ratio)
                logger.info(
                    "_split_train_test fallback by rows: train=%d test=%d",
                    k,
                    len(docs) - k,
                )
                return docs[:k], docs[k:]

            k = max(1, min(len(addrs) - 1, int(len(addrs) * self.train_ratio)))
            picked = set(random.sample(addrs, k)) if len(addrs) > 1 else set(addrs)
            train = [d for d in docs if d.get("X_address") in picked]
            test = [d for d in docs if d.get("X_address") not in picked]
            logger.info(
                "_split_train_test by address: unique_addrs=%d picked=%d -> train=%d test=%d",
                len(addrs),
                len(picked),
                len(train),
                len(test),
            )
            return train, test
        except Exception as exc:
            raise DataValidationError("Failed to split train/test").with_cause(exc)

    def _balanced_train_test_by_label(
        self, docs: list[TrainPairDoc], train_ratio: float, seed: int | None
    ) -> tuple[list[TrainPairDoc], list[TrainPairDoc]]:
        try:
            if not docs:
                return [], []

            rng = random.Random(seed)

            positives = [d for d in docs if d.get("Label") is True]
            negatives = [d for d in docs if d.get("Label") is not True]

            n_pos, n_neg = len(positives), len(negatives)
            logger.info("Class distribution: pos=%d neg=%d", n_pos, n_neg)
            if n_pos == 0 or n_neg == 0:
                k = int(len(docs) * max(0.0, min(1.0, train_ratio)))
                logger.info(
                    "Balancing not possible, fallback rows split: train=%d test=%d",
                    k,
                    len(docs) - k,
                )
                return docs[:k], docs[k:]

            base = min(n_pos, n_neg)
            n_per_class_train = max(0, int(base * max(0.0, min(1.0, train_ratio))))

            rng.shuffle(positives)
            rng.shuffle(negatives)

            train_pos = positives[:n_per_class_train]
            train_neg = negatives[:n_per_class_train]

            test_pos = positives[n_per_class_train:]
            test_neg = negatives[n_per_class_train:]

            train = train_pos + train_neg
            test = test_pos + test_neg

            if not train:
                k = int(len(docs) * max(0.0, min(1.0, train_ratio)))
                logger.info(
                    "Empty train after balancing, fallback rows split: train=%d test=%d",
                    k,
                    len(docs) - k,
                )
                return docs[:k], docs[k:]

            logger.info(
                "Balanced split: train_per_class=%d -> train=%d test=%d",
                n_per_class_train,
                len(train),
                len(test),
            )
            return train, test
        except Exception as exc:
            raise DataValidationError("Failed to balance train/test").with_cause(exc)

    def _write_output(
        self, col: Collection[TrainPairDoc], docs: list[TrainPairDoc]
    ) -> None:
        if not docs:
            logger.warning("No docs to write for %s", col.name)
            return

        try:
            seen: set[tuple[str, str, str]] = set()
            uniq_docs: list[TrainPairDoc] = []
            skipped = 0
            for d in docs:
                sg = d.get("_id", SG_FALLBACK)
                xa = d.get("X_address")
                ya = d.get("SubX_address")
                if not xa or not ya:
                    skipped += 1
                    continue
                k: tuple[str, str, str] = (sg, xa, ya)
                if k in seen:
                    continue
                seen.add(k)
                uniq_docs.append(d)
            if skipped:
                logger.debug(
                    "_write_output skipped %d docs due to missing addresses", skipped
                )
        except Exception as exc:
            raise DataValidationError("Failed to pre-deduplicate docs").with_cause(exc)

        ops: list[UpdateOne] = []
        batch_count = 0
        try:
            for d in uniq_docs:
                sg = d.get("_id", SG_FALLBACK)
                xa = d.get("X_address")
                ya = d.get("SubX_address")

                flt = {"_sg": sg, "X_address": xa, "SubX_address": ya}

                doc_out = dict(d)
                doc_out["_sg"] = doc_out.pop("_id", SG_FALLBACK)
                if "_id" in doc_out:
                    del doc_out["_id"]

                ops.append(UpdateOne(flt, {"$set": doc_out}, upsert=True))
                if len(ops) >= 1000:
                    _ = col.bulk_write(ops, ordered=False)  # pyright: ignore [reportUnknownMemberType]
                    batch_count += 1
                    logger.info("bulk_write batch #%d: %d ops", batch_count, 1000)
                    ops.clear()

            if ops:
                _ = col.bulk_write(ops, ordered=False)  # pyright: ignore [reportUnknownMemberType]
                batch_count += 1
                logger.info("bulk_write final batch #%d: %d ops", batch_count, len(ops))
        except pymongo_errors.BulkWriteError as exc:
            # Duplicate keys or other write issues
            # Detect duplicate-key sub-errors if present
            if any(
                err.get("code") in (11000, 11001)
                for err in exc.details.get("writeErrors", [])
            ):  # type: ignore[union-attr]
                raise DuplicateKeyError("Duplicate key during bulk write").with_cause(
                    exc
                )
            raise DatabaseWriteError("Bulk write failed").with_cause(exc)
        except Exception as exc:
            raise DatabaseWriteError("Failed to write output to Mongo").with_cause(exc)

        logger.info("Wrote %d unique docs into %s", len(uniq_docs), col.name)
