#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "alibi-detect",
#     "polars",
#     "pyarrow",
# ]
# [tool.uv]
# exclude-newer = "2025-09-23T00:00:00Z"
# ///

import logging
import argparse
from pathlib import Path
from pprint import pprint

import polars as pl
from alibi_detect.cd import KSDrift

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Kolmogorov-Smirnov Drift Test"
    )
    parser.add_argument(
        "--data-path",
        type=Path,
        default=Path(
            "data/samples/v1.1/mawiflow_samples_n1000_seed42_taxonomy_balanced.parquet"
        ),
        help="Path to the dataset parquet file",
    )
    parser.add_argument(
        "--numerical-features",
        nargs="+",
        default=[
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
            "Fwd PSH Flags",
            "Bwd PSH Flags",
            "Fwd URG Flags",
            "Bwd URG Flags",
            "Fwd RST Flags",
            "Bwd RST Flags",
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
        ],
    )
    parser.add_argument(
        "-y", "--reference-year", type=int, default=2007, help="Reference year"
    )
    parser.add_argument(
        "-p",
        "--p-value-threshold",
        type=float,
        default=0.05,
        help="P-value threshold for drift detection",
    )
    parser.add_argument(
        "-c",
        "--correction",
        type=str,
        choices=["bonferroni", "fdr"],
        default="fdr",
        help="Correction type for multivariate data",
    )
    parser.add_argument(
        "-a",
        "--alternative",
        type=str,
        choices=["two-sided", "less", "greater"],
        default="two-sided",
        help="Defines the alternative hypothesis",
    )
    parser.add_argument(
        "--drift-type",
        type=str,
        choices=["batch", "feature"],
        default="batch",
        help="Defines the type of drift to detect",
    )
    parser.add_argument(
        "--label-wise",
        action="store_true",
        help="Additionally predict drift per label",
    )
    parser.add_argument(
        "--year-column", type=str, default="year", help="Column name for year"
    )
    parser.add_argument(
        "--label-column",
        type=str,
        default="taxonomy_norm",
        help="Column name for labels if --label-wise is set",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output JSON file for predictions",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging"
    )
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    logger.debug(f"Arguments: {args}")

    logger.info(f"Loading dataset from {str(args.data_path)}...")
    data = pl.scan_parquet(args.data_path)
    available_years = (
        data.select(args.year_column)
        .unique()
        .sort(by=args.year_column)
        .collect()[args.year_column]
        .to_list()
    )
    logger.debug(f"Available years: {available_years}")

    if args.reference_year not in available_years:
        raise ValueError(
            f"Reference year {args.reference_year} not in available years {available_years}"
        )

    logger.info("Creating and fitting KSDrift detector...")
    cd = KSDrift(
        x_ref=data.filter(pl.col("year") == args.reference_year)
        .select(args.numerical_features)
        .collect()
        .to_numpy(),
        p_val=args.p_value_threshold,
        correction=args.correction,
        alternative=args.alternative,
    )

    preds = {
        "data_path": str(args.data_path),
        "numerical_features": args.numerical_features,
        "reference_year": args.reference_year,
        "p_val": args.p_value_threshold,
        "correction": args.correction,
        "alternative": args.alternative,
        "drift_type": args.drift_type,
    }

    for year in available_years[
        available_years.index(args.reference_year) + 1 :
    ]:
        logger.info(f"Predicting drift for year {year}...")
        preds[year] = cd.predict(
            data.filter(pl.col("year") == year)
            .select(args.numerical_features)
            .collect()
            .to_numpy(),
            drift_type=args.drift_type,
            return_p_val=True,
            return_distance=True,
        )

    if args.label_wise:
        logger.info("Starting label-wise processing...")
        labels = (
            data.select(args.label_column)
            .unique()
            .sort(by=args.label_column)
            .collect()[args.label_column]
            .to_list()
        )
        logger.debug(f"Found labels: {labels}")
        for label in labels:
            data_label = data.filter(pl.col(args.label_column) == label)
            if (
                data_label.filter(
                    pl.col(args.year_column) == args.reference_year
                )
                .select(args.numerical_features)
                .collect()
                .shape[0]
                < 2
            ):
                logger.warning(
                    f"Skipping label {label} for year {year} due to insufficient reference samples"
                )
                continue

            logger.info(f"Fitting label-wise detector for label {label}...")
            cd = KSDrift(
                x_ref=data.filter(pl.col("year") == args.reference_year)
                .select(args.numerical_features)
                .collect()
                .to_numpy(),
                p_val=args.p_value_threshold,
                correction=args.correction,
                alternative=args.alternative,
            )
            for year in available_years[
                available_years.index(args.reference_year) + 1 :
            ]:
                if year not in preds:
                    continue
                if "labels" not in preds[year]:
                    preds[year]["labels"] = {}
                if (
                    data_label.filter(pl.col(args.year_column) == year)
                    .select(args.numerical_features)
                    .collect()
                    .shape[0]
                    < 2
                ):
                    logger.warning(
                        f"Skipping label {label} for year {year} due to insufficient current samples"
                    )
                    continue
                logger.info(
                    f"Predicting drift for year {year} and label {label}..."
                )
                preds[year]["labels"][label] = cd.predict(
                    data_label.filter(pl.col(args.year_column) == year)
                    .select(args.numerical_features)
                    .collect()
                    .to_numpy(),
                    drift_type=args.drift_type,
                    return_p_val=True,
                    return_distance=True,
                )

    logger.info("Drift detection complete.")
    if args.output:
        logger.info(f"Writing results to {args.output}")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w") as f:
            pprint(preds, stream=f, indent=4)
    else:
        pprint(preds, indent=4)


if __name__ == "__main__":
    main()
