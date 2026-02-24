import logging
import argparse
from pathlib import Path
import duckdb
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine CICFlowMeter flows with annotations."
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug output"
    )
    parser.add_argument(
        "-a",
        "--annotations",
        type=Path,
        required=True,
        help="Path to the annotations directory",
    )
    parser.add_argument(
        "--annotations-file",
        type=str,
        default=Path("annotations.parquet"),
        help="Name of the annotations file (supports wildcards)",
    )
    parser.add_argument(
        "-f",
        "--flows",
        type=Path,
        required=True,
        help="Path to the flows directory",
    )
    parser.add_argument(
        "--flows-file",
        type=str,
        default="*_Flow.csv",
        help="Name of the flows file (supports wildcards)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Path to the output directory where the combined flows will be saved",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default="flows.parquet",
        help="Name of the output file",
    )
    return parser.parse_args()


def dir_exists(path: Path):
    if not path.is_dir():
        msg = f"Directory {path} does not exist or is not a directory."
        logger.error(msg)
        raise FileNotFoundError(msg)


def main():
    args = parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    logger.debug(f"{args=}")

    dir_exists(args.annotations)
    dir_exists(args.flows)

    conn = duckdb.connect(database=":memory:")

    # read annotations
    annotaions_path = args.annotations / args.annotations_file
    logger.info(f"Reading annotations from {annotaions_path}")
    conn.sql(f"""
        CREATE OR REPLACE VIEW v_annotations AS
        FROM '{annotaions_path}';
    """)
    logger.debug(
        conn.execute("""
        DESCRIBE v_annotations;
    """).fetchall()
    )
    logger.info(
        f"Found {conn.sql('SELECT COUNT(*) FROM v_annotations').fetchone()[0]} annotations."
    )

    # sort annotations by feature count and duration
    conn.sql("""
        CREATE OR REPLACE TABLE t_annotations AS
        SELECT
            *,
            (CASE WHEN start IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN stop IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN src_ip IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN src_port IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN dst_ip IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN dst_port IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN proto IS NOT NULL THEN 1 ELSE 0 END) as feature_count,
            stop - start as duration
        FROM v_annotations
        ORDER BY feature_count DESC, duration;
    """)

    # read flows
    flows_path = args.flows / args.flows_file
    logger.info(f"Reading flows from {flows_path}")
    conn.sql(f"""
        CREATE OR REPLACE VIEW v_flows AS
        SELECT *,
        CASE
            WHEN Protocol = 6 THEN 'tcp'
            WHEN Protocol = 17 THEN 'udp'
            WHEN Protocol = 1 THEN 'icmp'
            WHEN Protocol = 2 THEN 'igmp'
            WHEN Protocol = 4 THEN 'ip'
            WHEN Protocol = 41 THEN 'ipv6'
            WHEN Protocol = 58 THEN 'icmpv6'
            WHEN Protocol = 89 THEN 'ospf'
            WHEN Protocol = 132 THEN 'sctp'
            ELSE 'unknown_' || CAST(Protocol AS VARCHAR)
        END as protocol_name
        FROM '{flows_path}';
    """)
    logger.debug(
        conn.execute("""
        DESCRIBE v_flows;
    """).fetchall()
    )
    logger.info(
        f"Found {conn.sql('SELECT COUNT(*) FROM v_flows').fetchone()[0]} flows."
    )

    # combine flows with annotations based on feature count and duration
    logger.info("Combining flows with annotations...")
    conn.sql("""
        CREATE OR REPLACE VIEW v_flows_with_annotations AS
        WITH flow_annotation_matches AS (
            SELECT
                f.*,
                a.label as annotation_label,
                a.* EXCLUDE (label, start, stop, src_ip, src_port, dst_ip, dst_port, proto),
                -- Rank annotations by specificity (most features first, then shortest duration)
                ROW_NUMBER() OVER (
                    PARTITION BY f."Flow ID"
                    ORDER BY a.feature_count DESC, a.duration ASC NULLS LAST
                ) as annotation_rank
            FROM v_flows f
            JOIN t_annotations a ON
                -- Match time window if specified
                (a.start IS NULL OR epoch(f."Timestamp") >= a.start) AND
                (a.stop IS NULL OR epoch(f."Timestamp") <= a.stop) AND
                -- Match source IP if specified
                (a.src_ip IS NULL OR f."Src IP" = a.src_ip) AND
                -- Match source port if specified
                (a.src_port IS NULL OR f."Src Port" = a.src_port) AND
                -- Match destination IP if specified
                (a.dst_ip IS NULL OR f."Dst IP" = a.dst_ip) AND
                -- Match destination port if specified
                (a.dst_port IS NULL OR f."Dst Port" = a.dst_port) AND
                -- Match protocol if specified
                (a.proto IS NULL OR f.protocol_name = a.proto)
        ),
        best_annotations AS (
            SELECT *
            FROM flow_annotation_matches
            WHERE annotation_rank = 1
        )
        SELECT
            f.* EXCLUDE (Label, day, month, year),
            -- Override Label: Use annotation label if available, otherwise 'benign'
            COALESCE(b.annotation_label, 'benign') as "Label",
            b.rule_id,
            b.anomaly_id,
            b.distance_normal,
            b.distance_anomalous,
            b.heuristic,
            b.hough_sensitive,
            b.hough_optimal,
            b.hough_conservative,
            b.gamma_sensitive,
            b.gamma_optimal,
            b.gamma_conservative,
            b.kl_sensitive,
            b.kl_optimal,
            b.kl_conservative,
            b.pca_sensitive,
            b.pca_optimal,
            b.pca_conservative,
            b.taxonomy,
            b.distance,
            b.num_detectors,
            b.feature_count,
            b.duration,
            b.annotation_rank
        FROM v_flows f
        LEFT JOIN best_annotations b ON f."Flow ID" = b."Flow ID";
    """)
    logger.debug(
        conn.execute("""
        DESCRIBE v_flows_with_annotations;
    """).fetchall()
    )
    logger.info(
        f"Found {conn.sql('SELECT COUNT(*) FROM v_flows_with_annotations').fetchone()[0]} flows with annotations."
    )

    # write to disk
    os.makedirs(args.output, exist_ok=True)
    output_file = args.output / args.output_file
    logger.info(f"Writing combined flows with annotations to {output_file}")
    conn.sql(f"""
        COPY v_flows_with_annotations
        TO '{output_file}';
    """)
    logger.info("Combined flows successfully written!")


if __name__ == "__main__":
    main()
