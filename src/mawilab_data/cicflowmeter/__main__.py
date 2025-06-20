import logging
import argparse
import os
from pathlib import Path
import duckdb
import time
from mawilab_data.schema.cicflowmeter import CicFlowMeterSchema

logger = logging.getLogger(__name__)


def setup_argparser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CICFlowMeter Flow Processor")

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity level (up to 3 times)",
    )

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to the input files containing flow data",
    )

    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to the output directory for processed flow data",
    )

    parser.add_argument(
        "--annotations",
        type=str,
        required=True,
        help="Path to the annotations file",
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

    input_path = Path(args.input)
    if not input_path.exists():
        raise ValueError(f"Path {input_path} does not exist.")
    if not input_path.is_dir():
        raise ValueError(f"Path {input_path} is not a directory.")

    output_path = Path(args.output)
    if not output_path.exists():
        os.makedirs(output_path, exist_ok=True)
    if not output_path.is_dir():
        raise ValueError(f"Path {output_path} is not a directory.")

    annotations_path = Path(args.annotations)
    if not annotations_path.exists():
        raise ValueError(f"Path {annotations_path} does not exist.")
    if not annotations_path.is_file():
        raise ValueError(f"Path {annotations_path} is not a file.")

    db = duckdb.connect()

    logger.info(f"Reading flows from {input_path}/*.pcap_Flow.csv")
    db.sql(f"""
        CREATE OR REPLACE VIEW flows AS
            SELECT
                *,
                CASE
                    WHEN filename LIKE '%benign_None.pcap_Flow.csv%' THEN NULL
                    ELSE CAST(regexp_extract(filename, '.+?_([0-9]+)\\.pcap_Flow\\.csv', 1) AS USMALLINT)
                END AS 'Anomaly ID'
            FROM read_csv_auto(
                '{input_path}/*.pcap_Flow.csv',
                timestampformat = '%d/%m/%Y %I:%M:%S %p',
                header=true,
                sep=',',
                filename=true,
                sample_size = -1);
    """)
    logger.debug(f"flows schema:\n{db.sql('DESCRIBE flows')}")

    logger.info(f"Reading annotations from {annotations_path}")
    db.sql(f"""
        CREATE OR REPLACE VIEW annotations AS
            SELECT *
            FROM read_parquet('{annotations_path}');
    """)
    logger.debug(f"annotations schema:\n{db.sql('DESCRIBE annotations')}")

    output_filename = "cicflowmeter_flows.parquet"
    logger.info(f"Writing to {output_path}/{output_filename}")

    # Get formatted SQL expressions for all fields
    sql_cast_expressions = (
        CicFlowMeterSchema.get_formatted_sql_cast_expressions()
    )

    db.sql(f"""
        COPY (
            SELECT
                {sql_cast_expressions}
            FROM flows AS f
            LEFT JOIN annotations AS a
            ON f.Label = a.label AND f."Anomaly ID" = a.anomaly_id
            ORDER BY f.Timestamp ASC
        ) TO '{output_path}/{output_filename}' (
            FORMAT parquet,
            COMPRESSION lz4,
            KV_METADATA {{
                created_at: '{time.strftime("%Y-%m-%d %H:%M:%S")}',
                cicflowmeter_version: 'V3-0.0.4-SNAPSHOT'
            }}
        );
    """)


if __name__ == "__main__":
    main()
