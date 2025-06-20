import logging
import argparse
from mawilab_data.sampler.local import LocalSampler
from datetime import datetime

logger = logging.getLogger(__name__)


def setup_argparser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MawiLab Data Sampler")

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity level (up to 3 times)",
    )
    parser.add_argument(
        "-n",
        "--sample-size",
        type=int,
        required=True,
        help="Number of samples to generate",
    )
    parser.add_argument(
        "-s",
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2000-01-01",
        help="Start date for sampling (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2025-01-01",
        help="End date for sampling (YYYY-MM-DD)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Output path for the sampled data",
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
    logger.debug("Logging setup complete.")


def main():
    args = setup_argparser()
    setup_logging(args.verbose)

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    logger.info(
        f"Sampling data from {start_date} to {end_date} with seed {args.seed}"
    )

    sampler = LocalSampler()
    df = sampler.create_sample(
        sample_size=args.sample_size,
        seed=args.seed,
        start_date=start_date,
        end_date=end_date,
    )
    logger.info(f"Sampled {len(df)} rows of data.")

    output_path = args.output
    df.write_parquet(output_path, compression="lz4")
    logger.info(f"Sampled data written to {output_path}")


if __name__ == "__main__":
    main()
