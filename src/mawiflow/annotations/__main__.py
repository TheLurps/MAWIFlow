import os
import argparse
from pathlib import Path
import polars as pl
import logging
from .readers import CsvAnnotationReader, XmlAnnotationReader

pl.enable_string_cache()

logger = logging.getLogger(__name__)


def setup_argparser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine annotations for MAWILab dataset."
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity level (up to 3 times)",
    )

    parser.add_argument(
        "-r", "--raw", type=Path, required=True, help="Path to raw data"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Path to output directory",
    )

    return parser.parse_args()


def setup_logging(verbosity: int) -> None:
    LOGLEVEL = {
        0: logging.ERROR,
        1: logging.WARNING,
        2: logging.INFO,
        3: logging.DEBUG,
    }

    logging.basicConfig(level=LOGLEVEL.get(verbosity, logging.DEBUG))


def main():
    args = setup_argparser()
    setup_logging(args.verbose)

    input_path = Path(args.raw)
    if not input_path.exists():
        raise ValueError(f"Path {input_path} does not exist.")
    if not input_path.is_dir():
        raise ValueError(f"Path {input_path} is not a directory.")

    output_path = Path(args.output)
    if not output_path.exists():
        os.makedirs(output_path, exist_ok=True)
    if not output_path.is_dir():
        raise ValueError(f"Path {output_path} is not a directory.")

    # find all annotation files
    annotation_files = {}
    annotation_file_patterns = {
        "anomalous_suspicious_csv": "*_anomalous_suspicious.csv",
        "anomalous_suspicious_xml": "*_anomalous_suspicious.xml",
        "notice_csv": "*_notice.csv",
        "notice_xml": "*_notice.xml",
    }

    for type, pattern in annotation_file_patterns.items():
        files = list(input_path.glob(pattern))

        # anomalous_suspicious files must exist, notice files are optional
        if not files and type.startswith("anomalous_suspicious"):
            raise ValueError(f"No {type} files found in {input_path}.")

        # multiple matches are not allowed
        if len(files) > 1:
            raise ValueError(f"Multiple {type} files found in {input_path}.")

        if files:
            annotation_files[type] = files[0]

    # read all annotation files
    annotations = {}
    for type, file in annotation_files.items():
        logger.debug(f"Reading {file}...")

        df = None
        if file.name.endswith(".csv"):
            df = CsvAnnotationReader(file).read()
        elif file.name.endswith(".xml"):
            df = XmlAnnotationReader(file).read()

        if df is not None:
            logger.debug(f"Successfully read {file} with shape {df.shape}.")
            annotations[type] = df
        else:
            raise ValueError(f"Failed to read {file}.")

    # add sequential index
    for type, df in annotations.items():
        annotations[type] = df.with_row_index("rn")

    # combine annotations
    anomalous_suspicious = annotations["anomalous_suspicious_xml"].join(
        annotations["anomalous_suspicious_csv"].select(
            ["rn", "distance", "num_detectors"]
        ),
        on="rn",
    )
    if "notice_xml" in annotations and "notice_csv" in annotations:
        notice = annotations["notice_xml"].join(
            annotations["notice_csv"].select(
                ["rn", "distance", "num_detectors"]
            ),
            on="rn",
        )

        combined = pl.concat(
            [anomalous_suspicious.drop("rn"), notice.drop("rn")]
        ).with_row_index(name="rule_id", offset=0)

    else:
        logger.warning(
            "No notice annotations found, only anomalous_suspicious annotations will be used."
        )
        combined = anomalous_suspicious.with_row_index(name="rule_id", offset=0)

    logger.debug(f"Combined annotations: {combined.shape=}, {combined.schema=}")
    combined.write_parquet(
        output_path / "annotations.parquet",
        compression="lz4",
    )
    logger.info(
        f"Combined annotations written to {output_path / 'annotations.parquet'}."
    )


if __name__ == "__main__":
    main()
