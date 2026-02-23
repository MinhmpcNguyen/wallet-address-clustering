import re
import warnings
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from imblearn.over_sampling import SMOTE
from lightgbm.basic import Dataset
from numpy import float64, ndarray
from numpy._typing._array_like import NDArray
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from scipy.sparse._matrix import spmatrix
from sklearn.metrics import accuracy_score, classification_report, fbeta_score

from utils.logger_utils import get_logger

warnings.filterwarnings("ignore")
logger = get_logger("LightGBMTrainer")


class LightGBMTrainer:
    def __init__(
        self,
        drop_cols: list[str],
        smote_k: int = 5,
        num_leaves: int = 190,
        feature_fraction: float = 0.4,
        max_depth: int = 40,
    ):
        self.drop_cols: list[str] = drop_cols or []
        self.smote_k: int = smote_k
        self.num_leaves: int = num_leaves
        self.feature_fraction: float = feature_fraction
        self.max_depth: int = max_depth
        self.model = None
        self.feature_columns = None
        self.objective = None
        logger.info(
            "Initialized LightGBMTrainer(drop_cols=%s, smote_k=%d)",
            self.drop_cols,
            self.smote_k,
        )

    def _preprocess(self, df: pd.DataFrame):
        if df.empty:
            logger.error("Training dataframe is empty.")
            raise ValueError("Training dataframe is empty.")
        if "Label" not in df.columns:
            logger.error("Missing 'Label' column in dataset.")
            raise ValueError("Missing 'Label' column in dataset.")

        # Drop only columns that actually exist
        to_drop: list[str] = [c for c in self.drop_cols if c in df.columns]
        if to_drop:
            logger.debug("Dropping columns: %s", to_drop)
            df = df.drop(columns=to_drop)

        # Map label -> {0,1}
        label_map: dict[str | bool, int] = {"False": 0, "True": 1, False: 0, True: 1}
        df["Label"] = pd.Series(df["Label"]).replace(label_map)

        if bool(df["Label"].isnull().any()):
            bad: Any = df.loc[df["Label"].isnull(), "Label"]
            logger.error(
                "Invalid Label values that cannot be mapped to 0/1. Examples: %s",
                bad.head(3).tolist(),
            )
            raise ValueError(
                f"Invalid Label values that cannot be mapped to 0/1. Examples: {bad.head(3).tolist()}"
            )
        df["Label"] = pd.Series(df["Label"]).astype(int)
        logger.debug(
            "Preprocess complete: shape=%s, positives=%d, negatives=%d",
            df.shape,
            int((df["Label"] == 1).sum()),
            int((df["Label"] == 0).sum()),
        )
        return df

    def _prepare_features(
        self, df: pd.DataFrame, is_train: bool = False
    ) -> tuple[DataFrame, Series]:
        # --- Basic checks ---------------------------------------------------------
        if "Label" not in df.columns:
            logger.error("Missing 'Label' column after preprocessing.")
            raise ValueError("Missing 'Label' column after preprocessing.")

        X_raw = df.drop(columns=["Label"])
        if X_raw.shape[1] == 0:
            logger.error(
                "No feature columns left after dropping; check drop_cols and input data."
            )
            raise ValueError(
                "No feature columns left after dropping; check drop_cols and input CSVs."
            )

        # --- One-hot encode (guarded) --------------------------------------------
        try:
            features: DataFrame = pd.get_dummies(X_raw, drop_first=True)
        except Exception as e:
            logger.exception(f"pd.get_dummies failed: {e}")
            raise

        if features.shape[1] == 0:
            logger.error("No features after get_dummies; check categorical columns.")
            raise ValueError("No features after get_dummies; check categorical columns.")

        # Ensure numeric dtype and stable NaN handling
        # Use float32 to reduce memory footprint while keeping numeric type.
        try:
            if features.isnull().values.any():
                logger.debug("NaNs detected in features; filling with 0.")
                features = features.fillna(0)
            features = features.astype("float32", copy=False)
        except Exception as e:
            logger.exception(f"astype('float32') on features failed: {e}")
            raise

        # Prepare target
        try:
            y: Series = pd.Series(df["Label"])
        except Exception as e:
            logger.exception(f"Failed to extract target 'Label': {e}")
            raise

        x = features

        # --- Train vs. Inference paths -------------------------------------------
        if is_train:
            # Sanitize names before saving feature names
            try:
                sanitized = [re.sub(r"[^\w]", "_", c) for c in x.columns]
                # If sanitization causes duplicates, make them unique deterministically.
                seen = {}
                final_cols: list[str] = []
                for col in sanitized:
                    if col not in seen:
                        seen[col] = 0
                        final_cols.append(col)
                    else:
                        seen[col] += 1
                        final_cols.append(f"{col}__{seen[col]}")
                if len(final_cols) != len(set(final_cols)):
                    # Extremely unlikely after the above, but guard anyway.
                    logger.error("Duplicate column names after sanitization; aborting.")
                    raise ValueError("Duplicate column names after sanitization.")
                x.columns = final_cols
                self.feature_columns = x.columns
                logger.debug("Captured feature_columns (n=%d)", len(self.feature_columns))
            except Exception as e:
                logger.exception(f"Failed to sanitize/record training feature columns: {e}")
                raise
        else:
            # Inference: align to training features exactly
            if getattr(self, "feature_columns", None) is None:
                logger.error("feature_columns is not set. Call fit() first.")
                raise RuntimeError("feature_columns is not set. Call fit() first.")

            # Report alignment stats for easier debugging
            try:
                current_cols = set(x.columns.tolist())
                train_cols = list(self.feature_columns)
                missing = [c for c in train_cols if c not in current_cols]
                extra = [c for c in current_cols if c not in train_cols]
                logger.debug(
                    "Aligning features: current=%d, train=%d, missing=%d, extra=%d",
                    len(current_cols), len(train_cols), len(missing), len(extra)
                )
                # Align: add missing with 0, drop extras
                x = x.reindex(columns=self.feature_columns, fill_value=0)
            except Exception as e:
                logger.exception(f"Failed during feature alignment (reindex): {e}")
                raise

        # --- Final LightGBM name sanitization (idempotent) ------------------------
        try:
            new_cols: list[str] = [re.sub(r"[^\w]", "_", col) for col in x.columns.tolist()]
            if list(x.columns) != new_cols:
                logger.debug(
                    "Sanitized %d column names for LightGBM compatibility", len(new_cols)
                )
            x.columns = new_cols
        except Exception as e:
            logger.exception(f"Final column name sanitization failed: {e}")
            raise

        return x, y

    def fit(self, train_file: str) -> lgb.Booster:
        logger.info("Loading training data from: %s", train_file)
        df: DataFrame = pd.read_csv(train_file)
        if df.empty:
            logger.error("Training file '%s' is empty or cannot be read.", train_file)
            raise ValueError(
                f"Training file '{train_file}' is empty or cannot be read."
            )
        df = self._preprocess(df)
        x, y = self._prepare_features(df, is_train=True)

        # Store objective for evaluate()
        self.objective = "multiclass"

        cls_counts: pd.Series = y.value_counts()
        logger.info("Class distribution before SMOTE: %s", cls_counts.to_dict())
        X_resampled, y_resampled = x, y

        if len(cls_counts) >= 2:
            minority_count = int(cls_counts.min())
            k = min(self.smote_k, max(1, minority_count - 1))
            if minority_count >= 2 and k >= 1:
                try:
                    logger.info("Applying SMOTE with k_neighbors=%d", k)
                    smote = SMOTE(k_neighbors=k, random_state=42)
                    resampled = smote.fit_resample(x, y)
                    if len(resampled) == 2:
                        X_resampled, y_resampled = map(pd.DataFrame, resampled)
                    else:
                        raise ValueError(
                            "Unexpected return value from smote.fit_resample"
                        )
                    logger.info(
                        "After SMOTE: class distribution %s",
                        y_resampled.value_counts().to_dict(),
                    )
                except Exception as e:
                    logger.warning(
                        "SMOTE skipped due to: %s. Proceeding without resampling.", e
                    )

            params = {
                "objective": "multiclass",
                "metric": "multi_logloss",
                "num_classes": 2,
                "learning_rate": 0.05,
                "device": "gpu",  # will fallback if GPU unavailable
                "verbosity": -1,
                "boosting_type": "gbdt",
                "num_leaves": self.num_leaves,
                "feature_fraction": self.feature_fraction,
                "max_depth": self.max_depth,
            }
        else:
            # Single class -> switch to binary
            logger.warning(
                "Training set has a single class %s. Switching to binary objective.",
                cls_counts.to_dict(),
            )
            self.objective = "binary"
            params = {
                "objective": "binary",
                "metric": "binary_logloss",
                "learning_rate": 0.05,
                "device": "gpu",  # will fallback if GPU unavailable
                "verbosity": -1,
                "boosting_type": "gbdt",
                "num_leaves": self.num_leaves,
                "feature_fraction": self.feature_fraction,
                "max_depth": self.max_depth,
            }

        train_data: Dataset = lgb.Dataset(X_resampled, label=y_resampled)

        # Try GPU -> fallback to CPU when OpenCL/GPU not available
        try:
            logger.info("Training LightGBM model (device=%s)…", params.get("device"))
            self.model = lgb.train(params, train_data, num_boost_round=250)
        except lgb.basic.LightGBMError as e:
            msg = str(e)
            if "No OpenCL device found" in msg or "GPU" in msg or "OpenCL" in msg:
                logger.warning("No GPU/OpenCL available. Falling back to CPU.")
                params["device"] = "cpu"
                self.model = lgb.train(params, train_data, num_boost_round=250)
            else:
                logger.error("Training failed with LightGBMError: %s", e)
                raise
        logger.info("Training finished.")
        return self.model

    def evaluate(
        self, test_file: str
    ) -> dict[str, float | NDArray[float64] | dict[str, float] | str]:
        if self.model is None:
            logger.error("Model has not been trained. Call .fit() first.")
            raise RuntimeError("Model has not been trained. Call .fit() first.")

        logger.info("Loading test data from: %s", test_file)
        raw_test_df: DataFrame = pd.read_csv(test_file)
        if raw_test_df.empty:
            logger.error("Test file '%s' is empty or cannot be read.", test_file)
            raise ValueError(f"Test file '{test_file}' is empty or cannot be read.")
        test_df: DataFrame = self._preprocess(raw_test_df)
        X_test, y_test = self._prepare_features(test_df)

        logger.info("Predicting on test set…")
        y_pred: ndarray[Any, Any] | spmatrix | list[spmatrix] = self.model.predict(
            X_test
        )
        logger.info("Prediction finished.")

        # Decide task type
        is_multiclass = False
        if getattr(self, "objective", None) == "multiclass":
            is_multiclass = isinstance(y_pred, np.ndarray) and y_pred.ndim == 2 and y_pred.shape[1] > 1
        else:
            is_multiclass = isinstance(y_pred, np.ndarray) and y_pred.ndim == 2 and y_pred.shape[1] > 2

        if is_multiclass:
            # (n_samples, n_classes) -> (n_samples,)
            y_pred_labels = np.asarray(y_pred).argmax(axis=1).astype(int)
        else:
            # Binary: accept 1D probs or 2D with [:,1]
            if isinstance(y_pred, np.ndarray) and y_pred.ndim == 2:
                if y_pred.shape[1] == 1:
                    y_prob = np.asarray(y_pred, dtype=np.float64).ravel()
                else:
                    y_prob = np.asarray(y_pred[:, 1], dtype=np.float64)
            else:
                y_prob = np.asarray(y_pred, dtype=np.float64).ravel()

            # Vectorized threshold → (n_samples,) ints
            y_pred_labels = (y_prob >= 0.5).astype(int)

        # Ensure shapes & dtypes
        y_true = np.asarray(y_test, dtype=int).ravel()
        if y_true.shape[0] != y_pred_labels.shape[0]:
            raise ValueError(f"Shape mismatch: y_true={y_true.shape}, y_pred={y_pred_labels.shape}")

        f1: float | NDArray[float64] = fbeta_score(y_test, y_pred_labels, beta=0.5)
        acc = accuracy_score(y_test, y_pred_labels)
        report: dict[str, float] | str = classification_report(
            y_test, y_pred_labels, output_dict=True
        )

        logger.info("Evaluation — F1(beta=0.5)=%.4f | Accuracy=%.4f", f1, acc)
        logger.debug("Classification report: %s", report)

        # Also print for quick CLI use
        print(f"F1 Score (beta=0.5): {f1:.4f}")
        print(f"Accuracy: {acc:.4f}")
        return {"f1": f1, "accuracy": acc, "classification_report": report}

    def get_model(self):
        return self.model

    def train_and_evaluate(self, train_file: str, test_file: str):
        _ = self.fit(train_file)
        results = {}
        return self.model, results
