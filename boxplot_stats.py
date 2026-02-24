#! /usr/bin/env -S uv run
# -*- coding: utf-8 -*-

import logging
import argparse
from pathlib import Path
import os
import duckdb
import polars as pl

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def generate_sql(
    feature: str, taxonomy: str, table: str = "flows", year_col: str = "year"
) -> str:
    # Compute stats for a single taxonomy value (or benign as NULL / '')
    taxonomy_value = taxonomy.replace("'", "''")
    if taxonomy == "benign":
        where_clause = f"{feature} IS NOT NULL AND isfinite({feature}) AND (taxonomy IS NULL OR taxonomy='')"
    else:
        where_clause = f"{feature} IS NOT NULL AND isfinite({feature}) AND taxonomy = '{taxonomy_value}'"
    sql = f"""
        WITH base AS (
            SELECT
                CAST({year_col} AS INT) AS year,
                CAST({feature} AS DOUBLE) AS x
            FROM {table}
            WHERE {where_clause}
        ),
        q AS (
            SELECT
                year,
                quantile(x, 0.25) AS q1,
                quantile(x, 0.50) AS med,
                quantile(x, 0.75) AS q3,
                count(*)          AS n
            FROM base
            GROUP BY year
        ),
        fences AS (
            SELECT
                year, q1, med, q3, n,
                (q1 - 1.5 * (q3 - q1)) AS lower_fence,
                (q3 + 1.5 * (q3 - q1)) AS upper_fence
            FROM q
        ),
        wl AS (
            SELECT
                f.year,
                min(CASE WHEN b.x >= f.lower_fence THEN b.x END) AS whislo
            FROM base b JOIN fences f USING (year)
            GROUP BY f.year
        ),
        wh AS (
            SELECT
                f.year,
                max(CASE WHEN b.x <= f.upper_fence THEN b.x END) AS whishi
            FROM base b JOIN fences f USING (year)
            GROUP BY f.year
        )
        SELECT
            q.year,
            '{taxonomy_value}' AS taxonomy,
            q.q1, q.med, q.q3,
            COALESCE(wl.whislo, q.q1) AS whislo,
            COALESCE(wh.whishi, q.q3) AS whishi,
            q.n
        FROM q
        JOIN wl USING (year)
        JOIN wh USING (year)
        ORDER BY year;
    """
    return sql


def main():
    parser = argparse.ArgumentParser(
        description="Compute boxplot statistics for flow features."
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="boxplots.csv",
        help="Path to output CSV file for boxplot statistics (default: boxplots.csv)",
    )
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        default=Path("./data/flows"),
        help="Base path to flow parquet data (default: ./data/flows)",
    )
    parser.add_argument(
        "-k",
        "--key",
        type=str,
        default="v1.1/year=*/month=*/day=*/flows.parquet",
        help="Relative glob / partition pattern for parquet files (default: v1.1/year=*/month=*/day=*/flows.parquet)",
    )
    args = parser.parse_args()

    if not args.input.exists() or not args.input.is_dir():
        raise ValueError(
            f"Input path '{args.input}' does not exist or is not a directory."
        )

    conn = duckdb.connect(":memory:")
    conn.sql(f"""
        CREATE OR REPLACE VIEW flows AS
        FROM read_parquet(
            '{str(args.input)}/{args.key}',
            hive_partitioning=True);
    """)

    features = [
        "Flow Duration",
        "Total Fwd Packet",
        "Total Bwd packets",
        "Total Length of Fwd Packet",
        "Total Length of Bwd Packet",
        "Fwd Packet Length Max",
        "Fwd Packet Length Min",
        "Fwd Packet Length Mean",
        "Fwd Packet Length Std",
        "Bwd Packet Length Max",
        "Bwd Packet Length Min",
        "Bwd Packet Length Mean",
        "Bwd Packet Length Std",
        "Flow Bytes/s",
        "Flow Packets/s",
        "Flow IAT Mean",
        "Flow IAT Std",
        "Flow IAT Max",
        "Flow IAT Min",
        "Fwd IAT Total",
        "Fwd IAT Mean",
        "Fwd IAT Std",
        "Fwd IAT Max",
        "Fwd IAT Min",
        "Bwd IAT Total",
        "Bwd IAT Mean",
        "Bwd IAT Std",
        "Bwd IAT Max",
        "Bwd IAT Min",
        "Fwd Header Length",
        "Bwd Header Length",
        "Fwd Packets/s",
        "Bwd Packets/s",
        "Packet Length Min",
        "Packet Length Max",
        "Packet Length Mean",
        "Packet Length Std",
        "Packet Length Variance",
        "FIN Flag Count",
        "SYN Flag Count",
        "RST Flag Count",
        "PSH Flag Count",
        "ACK Flag Count",
        "URG Flag Count",
        "CWR Flag Count",
        "ECE Flag Count",
        "Down/Up Ratio",
        "Average Packet Size",
        "Fwd Segment Size Avg",
        "Bwd Segment Size Avg",
        "Fwd Bytes/Bulk Avg",
        "Fwd Packet/Bulk Avg",
        "Fwd Bulk Rate Avg",
        "Bwd Bytes/Bulk Avg",
        "Bwd Packet/Bulk Avg",
        "Bwd Bulk Rate Avg",
        "Subflow Fwd Packets",
        "Subflow Fwd Bytes",
        "Subflow Bwd Packets",
        "Subflow Bwd Bytes",
        "FWD Init Win Bytes",
        "Bwd Init Win Bytes",
        "Fwd Act Data Pkts",
        "Bwd Act Data Pkts",
        "Fwd Seg Size Min",
        "Bwd Seg Size Min",
        "Active Mean",
        "Active Std",
        "Active Max",
        "Active Min",
        "Idle Mean",
        "Idle Std",
        "Idle Max",
        "Idle Min",
        "Fwd TCP Retrans. Count",
        "Bwd TCP Retrans. Count",
        "Total TCP Retrans. Count",
        "Total Connection Flow Time",
    ]

    taxonomies = [
        t[0]
        for t in conn.execute("""
        SELECT DISTINCT taxonomy
        FROM flows
        WHERE taxonomy IS NOT NULL AND taxonomy != ''
        ORDER BY taxonomy;
    """).fetchall()
    ]
    taxonomies.append("benign")
    logger.debug(f"Identified taxonomies: {taxonomies}")

    frames = []
    for taxonomy in taxonomies:
        for feature in features:
            logger.info(
                f"Computing boxplot stats for feature '{feature}' and taxonomy '{taxonomy}'"
            )
            sql = generate_sql(f'"{feature}"', taxonomy)
            logger.debug(f"Executing SQL:{sql}")
            result = conn.execute(sql).pl()
            if not result.is_empty():
                # annotate feature
                result = result.with_columns(pl.lit(feature).alias("feature"))
                frames.append(result)

    if frames:
        all_df = pl.concat(frames, how="vertical")
    else:
        all_df = pl.DataFrame()
    logger.info(f"Total combined rows: {all_df.height}")
    os.makedirs(Path(args.output).parent, exist_ok=True)
    all_df.write_csv(args.output)
    logger.info(f"Boxplot stats written to {args.output}")


if __name__ == "__main__":
    main()
