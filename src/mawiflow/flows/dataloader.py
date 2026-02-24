import logging
from pathlib import Path
from typing import List, Optional
import duckdb
import numpy as np
import polars as pl

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(
        self,
        data_path: Path,
        data_key: str,
        features: Optional[List[str]] = None,
        batch_size: int = 10_000,
        normalize: bool = False,
        scaler: str = "MinMaxScaler",
        one_hot_encode: bool = False,
    ):
        logger.info("Initializing DataLoader")

        if not data_path.exists() or not data_path.is_dir():
            raise ValueError(
                f"Data path '{data_path}' does not exist or is not a directory."
            )

        self.conn = duckdb.connect(":memory:")
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW flows AS
            FROM read_parquet(
                '{str(data_path)}/{data_key}',
                hive_partitioning=True);
        """)
        self.dataset_len = self.conn.execute(
            "SELECT COUNT(*) FROM flows"
        ).fetchone()[0]
        self.batch_size = batch_size
        self.num_batches = int(np.ceil(self.dataset_len / self.batch_size))

        if features is None or "*" in features:
            self.features = [
                column[0]
                for column in self.conn.execute("DESCRIBE flows;").fetchall()
            ]
        else:
            self.features = features

        self.numeric_features = []
        self.categorical_features = []
        self.normalize = normalize
        self.scaler_type = scaler
        self.one_hot_encode = one_hot_encode
        self._minmax_scale = {}
        self._minmax_offset = {}
        self._standard_mean = {}
        self._standard_std = {}
        self.category_values = {}  # feature -> [categories]

        for column in self.conn.execute("DESCRIBE flows;").fetchall():
            feature = column[0]
            dtype = column[1]

            if feature in self.features or "*" in self.features:
                if dtype in [
                    "BIGINT",
                    "BOOLEAN",
                    "DECIMAL",
                    "DOUBLE",
                    "FLOAT",
                    "HUGEINT",
                    "INTEGER",
                    "SMALLINT",
                    "TINYINT",
                    "UBIGINT",
                    "UHUGEINT",
                    "UINTEGER",
                    "USMALLINT",
                    "UTINYINT",
                ]:
                    self.numeric_features.append(feature)
                elif dtype in ["VARCHAR"]:
                    self.categorical_features.append(feature)
                else:
                    logger.warning(
                        f"Feature '{feature}' has unsupported type '{dtype}' and will be ignored."
                    )

        if normalize and self.numeric_features:
            if scaler == "MinMaxScaler":
                stats_query = f"""
                    SELECT
                        {", ".join([f'MIN("{f}") AS "{f}_min", MAX("{f}") AS "{f}_max"' for f in self.numeric_features])}
                    FROM flows
                """
                stats_row = self.conn.execute(stats_query).fetchone()
                data_mins = []
                data_maxs = []
                for i in range(0, len(stats_row), 2):
                    data_mins.append(stats_row[i])
                    data_maxs.append(stats_row[i + 1])
                data_min_ = np.array(data_mins, dtype=float)
                data_max_ = np.array(data_maxs, dtype=float)
                data_range_ = data_max_ - data_min_
                data_range_[data_range_ == 0.0] = 1.0
                feature_range = (0.0, 1.0)
                scale_ = (feature_range[1] - feature_range[0]) / data_range_
                min_offset_ = feature_range[0] - data_min_ * scale_
                # Store per-feature
                for idx, feat in enumerate(self.numeric_features):
                    self._minmax_scale[feat] = float(scale_[idx])
                    self._minmax_offset[feat] = float(min_offset_[idx])
            elif scaler == "StandardScaler":
                stats_query = f"""
                    SELECT
                        {", ".join([f'AVG("{f}") AS "{f}_mean", STDDEV_SAMP("{f}") AS "{f}_std"' for f in self.numeric_features])}
                    FROM flows
                """
                stats_row = self.conn.execute(stats_query).fetchone()
                for i, feat in enumerate(self.numeric_features):
                    mean_val = stats_row[2 * i]
                    std_val = stats_row[2 * i + 1]
                    if std_val in (None, 0):
                        std_val = 1.0
                    self._standard_mean[feat] = float(mean_val)
                    self._standard_std[feat] = float(std_val)
            else:
                raise ValueError(
                    f"Scaler '{scaler}' is not supported. Choose 'MinMaxScaler' or 'StandardScaler'."
                )
            if one_hot_encode and self.categorical_features:
                sample_query = f"""
                    SELECT DISTINCT {", ".join([f'"{f}"' for f in self.categorical_features])}
                    FROM flows
                    WHERE {" AND ".join([f'"{f}" IS NOT NULL' for f in self.categorical_features])}
                    LIMIT 1000000;
                """
                sample_df = self.conn.sql(sample_query).pl()
                for feat in self.categorical_features:
                    cats = (
                        sample_df.select(pl.col(feat))
                        .drop_nulls()
                        .unique()
                        .to_series()
                        .to_list()
                    )
                    self.category_values[feat] = cats
        logger.info(
            f"DataLoader initialized with {self.dataset_len:,} rows, "
            f"{len(self.numeric_features):,} numeric features, "
            f"{len(self.categorical_features):,} categorical features, "
            f"batch size {self.batch_size:,}, total {self.num_batches:,} batches."
        )

    def __iter__(self):
        for counter in range(self.num_batches):
            logger.debug(
                f"Querying data (batch {counter + 1}/{self.num_batches})"
            )
            query = f"""
                SELECT {", ".join([f'"{feature}"' for feature in self.features])}
                FROM flows
                LIMIT {self.batch_size}
                OFFSET {counter * self.batch_size};
            """
            logger.debug(f"Executing query: {query}")
            df = self.conn.sql(query).pl()
            # Normalization
            if self.normalize and self.numeric_features:
                if self.scaler_type == "MinMaxScaler" and self._minmax_scale:
                    df = df.with_columns(
                        [
                            (
                                pl.col(f).cast(pl.Float64)
                                * self._minmax_scale[f]
                                + self._minmax_offset[f]
                            ).alias(f)
                            for f in self.numeric_features
                        ]
                    )
                elif (
                    self.scaler_type == "StandardScaler" and self._standard_mean
                ):
                    df = df.with_columns(
                        [
                            (
                                (
                                    pl.col(f).cast(pl.Float64)
                                    - self._standard_mean[f]
                                )
                                / self._standard_std[f]
                            ).alias(f)
                            for f in self.numeric_features
                        ]
                    )
            # One-hot encoding
            if self.one_hot_encode and self.category_values:
                for feat, cats in self.category_values.items():
                    df = df.with_columns(
                        [
                            (pl.col(feat) == cat)
                            .cast(pl.Boolean)
                            .alias(f"{feat}_{cat}")
                            for cat in cats
                        ]
                    )
                df = df.drop(self.categorical_features)
            logger.debug(f"DataFrame with shape {df.shape} loaded")
            yield df
