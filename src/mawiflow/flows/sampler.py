from typing import Union
import logging
import polars as pl
from pathlib import Path
import argparse
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Sampler:
    """A class for sampling network flow data with taxonomy-based balancing.

    This class provides functionality to sample network flow data from parquet files
    organized in a hierarchical directory structure (year/month/day). It supports
    both taxonomy-balanced sampling (equal samples per taxonomy) and malicious-benign
    balanced sampling (equal malicious vs benign samples).

    Attributes:
        data_path (Path): Path to the root data directory containing parquet files.
        sample_path (Path): Path where sampled data will be saved.
        year (str): Year to sample data from.
        n (int): Number of samples per taxonomy category.
        seed (int): Random seed for reproducible sampling.
        balance_malicious_benign (bool): Whether to balance malicious vs benign samples.
        data_pattern (Path): Full pattern path for data location.
    """

    def __init__(
        self,
        data_path: Union[Path, str],
        sample_path: Union[Path, str],
        year: Union[int, str],
        n: int = 100,
        seed: int = 42,
        balance_malicious_benign: bool = False,
    ):
        """Initialize the Sampler with configuration parameters.

        Args:
            data_path: Path to the data directory containing parquet files.
            sample_path: Path to the output directory for samples.
            year: Year to sample data from.
            n: Number of samples per taxonomy category. Defaults to 100.
            seed: Random seed for reproducible sampling. Defaults to 42.
            balance_malicious_benign: If True, balance malicious vs benign samples.
                If False, sample n entries per taxonomy including benign. Defaults to False.

        Raises:
            ValueError: If data_path doesn't exist or sample_path is not a directory.
        """
        self.data_path = (
            data_path if isinstance(data_path, Path) else Path(data_path)
        )
        self.data_key = f"v1.1/year={year}/month=*/day=*/flows.parquet"
        self.year = str(year)
        self.n = n
        self.seed = seed
        self.balance_malicious_benign = balance_malicious_benign

        if not self.data_path.exists() or not self.data_path.is_dir():
            msg = f"Data path {self.data_path} does not exist."
            logger.error(msg)
            raise ValueError(msg)

        self.sample_path = (
            sample_path if isinstance(sample_path, Path) else Path(sample_path)
        )
        if not self.sample_path.exists():
            self.sample_path.mkdir(parents=True, exist_ok=True)
        if not self.sample_path.is_dir():
            msg = f"Sample path {self.sample_path} is not a directory."
            logger.error(msg)
            raise ValueError(msg)

        self.data_pattern = self.data_path / self.data_key
        logger.info(f"Data pattern: {self.data_pattern}")

    def _load_month_data(self, month: str) -> pl.DataFrame:
        """Load data for a specific month (legacy method).

        Args:
            month: Month identifier (e.g., '01', '02').

        Returns:
            DataFrame containing filtered flow data for the month.

        Raises:
            Exception: If data loading fails.
        """
        pattern = str(
            self.data_path
            / f"v1.1/year={self.year}/month={month}/day=*/flows.parquet"
        )
        try:
            df = pl.scan_parquet(pattern).collect()
            return df.filter(pl.col("Label").is_in(["benign", "anomalous"]))
        except Exception as e:
            logger.error(f"Failed to load data for month {month}: {e}")
            raise

    def _group_taxonomy(self, taxonomy: str) -> str:
        """Group taxonomy labels into broader categories based on prefixes.

        Maps specific taxonomy labels to broader categories using prefix matching.
        Handles cases where taxonomy is None or 'benign'.

        Args:
            taxonomy: The original taxonomy label to categorize.

        Returns:
            The grouped taxonomy category name, or original taxonomy if no match found.

        Categories:
            - Unknown: labels starting with "unk", "empty"
            - Other: labels starting with "ttl_error", "hostout", "netout", "icmp_error"
            - HTTP: labels starting with "alphflhttp", "ptmphttp", "mptphttp", etc.
            - Multi. points: labels starting with "ptmp", "mptp", "mptmp"
            - Alpha flow: labels starting with "alphfl", "malphfl", "salphfl", etc.
            - IPv6 tunneling: labels starting with "ipv4gretun", "ipv46tun"
            - Port scan: labels starting with "posca", "ptpposca"
            - Network scan ICMP: labels starting with "ntscic", "dntscic"
            - Network scan UDP: labels starting with "ntscudp", "ptpposcaudp"
            - Network scan TCP: labels starting with "ntscack", "ntscsyn", etc.
            - DoS: labels starting with "dos", "distributed_dos", "ptpdos", etc.
            - benign: for benign traffic or None values
        """
        if taxonomy == "benign" or taxonomy is None:
            return "benign"

        taxonomy_lower = taxonomy.lower()

        # Unknown
        if taxonomy_lower.startswith(("unk", "empty")):
            return "Unknown"

        # Other
        if taxonomy_lower.startswith(
            ("ttl_error", "hostout", "netout", "icmp_error")
        ):
            return "Other"

        # HTTP
        if taxonomy_lower.startswith(
            ("alphflhttp", "ptmphttp", "mptphttp", "ptmplahttp", "mptplahttp")
        ):
            return "HTTP"

        # Multi. points
        if taxonomy_lower.startswith(("ptmp", "mptp", "mptmp")):
            return "Multi. points"

        # Alpha flow
        if taxonomy_lower.startswith(
            ("alphfl", "malphfl", "salphfl", "point_to_point", "heavy_hitter")
        ):
            return "Alpha flow"

        # IPv6 tunneling
        if taxonomy_lower.startswith(("ipv4gretun", "ipv46tun")):
            return "IPv6 tunneling"

        # Port scan
        if taxonomy_lower.startswith(("posca", "ptpposca")):
            return "Port scan"

        # Network scan ICMP
        if taxonomy_lower.startswith(("ntscic", "dntscic")):
            return "Network scan ICMP"

        # Network scan UDP
        if taxonomy_lower.startswith(("ntscudp", "ptpposcaudp")):
            return "Network scan UDP"

        # Network scan TCP
        if taxonomy_lower.startswith(
            (
                "ntscack",
                "ntscsyn",
                "sntscsyn",
                "ntsctcp",
                "ntscnull",
                "ntscxmas",
                "ntscfin",
                "dntscsyn",
            )
        ):
            return "Network scan TCP"

        # DoS
        if taxonomy_lower.startswith(
            ("dos", "distributed_dos", "ptpdos", "sptpdos", "ddos", "rflat")
        ):
            return "DoS"

        # If no match found, return original taxonomy
        return taxonomy

    def _load_month_data_lazy(self, month: str) -> pl.LazyFrame:
        """Load data for a specific month using lazy evaluation (legacy method).

        Args:
            month: Month identifier (e.g., '01', '02').

        Returns:
            LazyFrame with filtered data and taxonomy normalization applied.

        Raises:
            Exception: If data loading fails.
        """
        pattern = str(
            self.data_path
            / f"v1.1/year={self.year}/month={month}/day=*/flows.parquet"
        )
        try:
            return (
                pl.scan_parquet(pattern)
                .filter(pl.col("Label").is_in(["benign", "anomalous"]))
                .with_columns(
                    [
                        pl.col("taxonomy")
                        .fill_null("benign")
                        .alias("taxonomy_orig"),
                        pl.col("taxonomy")
                        .fill_null("benign")
                        .map_elements(
                            self._group_taxonomy, return_dtype=pl.String
                        )
                        .alias("taxonomy_norm"),
                    ]
                )
            )
        except Exception as e:
            logger.error(f"Failed to load data for month {month}: {e}")
            raise

    def _load_day_data_lazy(self, month: str, day: str) -> pl.LazyFrame:
        """Load data for a specific day using lazy evaluation.

        This method loads parquet data for a single day, applies filtering for
        benign/anomalous labels, and adds taxonomy normalization columns.

        Args:
            month: Month identifier (e.g., '01', '02').
            day: Day identifier (e.g., '01', '15', '31').

        Returns:
            LazyFrame with filtered data and added columns:
                - taxonomy_orig: Original taxonomy values with nulls filled as 'benign'
                - taxonomy_norm: Grouped taxonomy categories using _group_taxonomy()
            Returns empty LazyFrame if loading fails.
        """
        pattern = str(
            self.data_path
            / f"v1.1/year={self.year}/month={month}/day={day}/flows.parquet"
        )
        try:
            return (
                pl.scan_parquet(pattern)
                .filter(pl.col("Label").is_in(["benign", "anomalous"]))
                .with_columns(
                    [
                        pl.col("taxonomy")
                        .fill_null("benign")
                        .alias("taxonomy_orig"),
                        pl.col("taxonomy")
                        .fill_null("benign")
                        .map_elements(
                            self._group_taxonomy, return_dtype=pl.String
                        )
                        .alias("taxonomy_norm"),
                    ]
                )
            )
        except Exception as e:
            logger.debug(
                f"Failed to load data for month {month}, day {day}: {e}"
            )
            return pl.LazyFrame()

    def _get_available_months(self) -> list[str]:
        """Get list of available months for the specified year.

        Scans the data directory structure to find all available month directories
        for the configured year.

        Returns:
            Sorted list of month identifiers (e.g., ['01', '02', '03']).

        Raises:
            ValueError: If no data found for the specified year.
        """
        year_path = self.data_path / f"v1.1/year={self.year}"
        if not year_path.exists():
            raise ValueError(f"No data found for year {self.year}")

        months = []
        for month_dir in sorted(year_path.glob("month=*")):
            month = month_dir.name.split("=")[1]
            months.append(month)

        return months

    def _get_available_days(self, month: str) -> list[str]:
        """Get list of available days for the specified year and month.

        Args:
            month: Month identifier to scan for days.

        Returns:
            Sorted list of day identifiers (e.g., ['01', '02', '15']).
            Returns empty list if month directory doesn't exist.
        """
        month_path = self.data_path / f"v1.1/year={self.year}/month={month}"
        if not month_path.exists():
            return []

        days = []
        for day_dir in sorted(month_path.glob("day=*")):
            day = day_dir.name.split("=")[1]
            days.append(day)

        return days

    def _sample_from_lazy_df(
        self, lazy_df: pl.LazyFrame, taxonomy: str, n_samples: int
    ) -> pl.DataFrame:
        """Sample n_samples from a lazy dataframe for a specific taxonomy.

        Uses deterministic sampling based on hash of row indices combined with
        the configured seed to ensure reproducible results.

        Args:
            lazy_df: LazyFrame to sample from.
            taxonomy: Taxonomy category to filter and sample.
            n_samples: Number of samples to collect.

        Returns:
            DataFrame containing the sampled rows, or empty DataFrame if sampling fails.
        """
        try:
            return (
                lazy_df.filter(pl.col("taxonomy_norm") == taxonomy)
                .with_row_index("idx")
                .with_columns(
                    (pl.col("idx") + self.seed).hash().alias("hash_val")
                )
                .sort("hash_val")
                .head(n_samples)
                .drop(["idx", "hash_val"])
                .collect(engine="streaming")
            )
        except Exception as e:
            logger.debug(f"Failed to sample from taxonomy {taxonomy}: {e}")
            return pl.DataFrame()

    def _concat_with_schema_alignment(
        self, dfs: list[pl.DataFrame]
    ) -> pl.DataFrame:
        """Concatenate DataFrames with schema alignment to handle type mismatches.

        Attempts regular concatenation first. If schema errors occur due to type
        mismatches between DataFrames, aligns all DataFrames to the schema of
        the first DataFrame by casting columns to matching types.

        Args:
            dfs: List of DataFrames to concatenate.

        Returns:
            Single concatenated DataFrame with aligned schemas.
            Returns empty DataFrame if input list is empty.
            Returns single DataFrame if only one input DataFrame.
        """
        if not dfs:
            return pl.DataFrame()

        if len(dfs) == 1:
            return dfs[0]

        try:
            # Try regular concat first
            return pl.concat(dfs)
        except pl.exceptions.SchemaError as e:
            logger.debug(
                f"Schema error during concat, attempting alignment: {e}"
            )

            # Get the schema from the first dataframe
            base_schema = dfs[0].schema

            # Align all dataframes to the base schema
            aligned_dfs = []
            for df in dfs:
                aligned_df = df
                for col_name, expected_dtype in base_schema.items():
                    if col_name in df.columns:
                        current_dtype = df.schema[col_name]
                        if current_dtype != expected_dtype:
                            logger.debug(
                                f"Converting {col_name} from {current_dtype} to {expected_dtype}"
                            )
                            aligned_df = aligned_df.with_columns(
                                pl.col(col_name).cast(expected_dtype)
                            )
                aligned_dfs.append(aligned_df)

            return pl.concat(aligned_dfs)

    def sample(self):
        """Execute the sampling process for all available months.

        Main method that orchestrates the sampling process:
        1. Discovers available months for the configured year
        2. For each month, processes all available days
        3. Collects samples according to the balancing strategy
        4. Saves results to parquet files

        Balancing strategies:
        - balance_malicious_benign=False: n samples per taxonomy (including benign)
        - balance_malicious_benign=True: n samples per non-benign taxonomy +
          equal total of benign samples

        Output files:
        - taxonomy_balanced.parquet: When balance_malicious_benign=False
        - malicious_benign_balanced.parquet: When balance_malicious_benign=True
        """
        logger.info(
            f"Creating sample for year {self.year}, n={self.n}, seed={self.seed}."
        )

        months = self._get_available_months()

        for month in months:
            start_time = time.perf_counter()
            logger.info(f"Processing month {month}.")

            # Get available days for this month
            days = self._get_available_days(month)
            if not days:
                logger.warning(f"No days found for month {month}")
                continue

            # Initialize taxonomy sample collectors
            taxonomy_samples = {}
            taxonomy_targets = {}

            # Process each day to collect samples
            for day in days:
                day_start = time.perf_counter()
                lazy_df = self._load_day_data_lazy(month, day)

                # Skip if no data
                try:
                    row_count = (
                        lazy_df.select(pl.len())
                        .collect(engine="streaming")
                        .item()
                    )
                    if row_count == 0:
                        continue
                except Exception as e:
                    logger.error(f"Failed to get row count for day {day}: {e}")
                    continue

                # Get taxonomies for this day
                try:
                    taxonomies = (
                        lazy_df.select("taxonomy_norm")
                        .unique()
                        .collect(engine="streaming")
                        .to_series()
                        .to_list()
                    )
                except Exception as e:
                    logger.error(f"Failed to get taxonomies for day {day}: {e}")
                    continue

                # Filter taxonomies based on balance_malicious_benign setting
                if self.balance_malicious_benign:
                    # Only sample non-benign taxonomies during this phase
                    taxonomies_to_sample = [
                        t for t in taxonomies if t != "benign"
                    ]
                else:
                    # Sample all taxonomies including benign
                    taxonomies_to_sample = taxonomies

                # Sample from each taxonomy
                for taxonomy in taxonomies_to_sample:
                    if taxonomy not in taxonomy_samples:
                        taxonomy_samples[taxonomy] = []
                        taxonomy_targets[taxonomy] = self.n

                    # Calculate how many more samples we need
                    current_count = sum(
                        len(df) for df in taxonomy_samples[taxonomy]
                    )
                    needed = taxonomy_targets[taxonomy] - current_count

                    if needed > 0:
                        sample_df = self._sample_from_lazy_df(
                            lazy_df, taxonomy, needed
                        )
                        if not sample_df.is_empty():
                            taxonomy_samples[taxonomy].append(sample_df)
                            logger.debug(
                                f"Day {day}: Sampled {len(sample_df)} from {taxonomy}"
                            )

                day_end = time.perf_counter()
                logger.debug(
                    f"Processed day {day} in {day_end - day_start:.2f}s"
                )

            # Create output directory
            out_path = (
                self.sample_path
                / "v1.1"
                / f"year={self.year}"
                / f"month={month}"
                / f"n={self.n}"
                / f"seed={self.seed}"
            )
            out_path.mkdir(parents=True, exist_ok=True)

            # Combine samples and save
            if self.balance_malicious_benign:
                # Get all non-benign samples
                non_benign_dfs = []
                for taxonomy, sample_list in taxonomy_samples.items():
                    if sample_list:
                        combined = self._concat_with_schema_alignment(
                            sample_list
                        ).head(self.n)
                        non_benign_dfs.append(combined)
                        logger.info(
                            f"Collected {len(combined)} samples from taxonomy '{taxonomy}'"
                        )

                # Calculate total non-benign samples
                total_non_benign = sum(len(df) for df in non_benign_dfs)

                # Now sample benign equal to total non-benign
                benign_samples = []
                if total_non_benign > 0:
                    for day in days:
                        lazy_df = self._load_day_data_lazy(month, day)
                        try:
                            row_count = (
                                lazy_df.select(pl.len())
                                .collect(engine="streaming")
                                .item()
                            )
                            if row_count == 0:
                                continue
                        except Exception as e:
                            logger.error(
                                f"Failed to get row count for day {day}: {e}"
                            )
                            continue

                        current_benign_count = sum(
                            len(df) for df in benign_samples
                        )
                        needed_benign = total_non_benign - current_benign_count

                        if needed_benign > 0:
                            benign_df = self._sample_from_lazy_df(
                                lazy_df, "benign", needed_benign
                            )
                            if not benign_df.is_empty():
                                benign_samples.append(benign_df)

                        # Break if we have enough benign samples
                        if (
                            sum(len(df) for df in benign_samples)
                            >= total_non_benign
                        ):
                            break

                # Combine all samples
                if non_benign_dfs and benign_samples:
                    benign_combined = self._concat_with_schema_alignment(
                        benign_samples
                    ).head(total_non_benign)
                    all_samples = non_benign_dfs + [benign_combined]
                    combined_df = self._concat_with_schema_alignment(
                        all_samples
                    ).sort("Timestamp")

                    output_file = out_path / "malicious_benign_balanced.parquet"
                    combined_df.write_parquet(output_file, compression="zstd")
                    logger.info(
                        f"Saved {len(combined_df)} malicious-benign balanced samples to {output_file}"
                    )
                    logger.info(
                        f"Total non-benign: {total_non_benign}, Total benign: {len(benign_combined)}"
                    )

            else:
                # Regular taxonomy balanced sampling: n samples per taxonomy (including benign)
                all_samples = []
                for taxonomy, sample_list in taxonomy_samples.items():
                    if sample_list:
                        combined = self._concat_with_schema_alignment(
                            sample_list
                        ).head(self.n)
                        all_samples.append(combined)
                        logger.info(
                            f"Collected {len(combined)} samples from taxonomy '{taxonomy}'"
                        )

                if all_samples:
                    combined_df = self._concat_with_schema_alignment(
                        all_samples
                    ).sort("Timestamp")
                    output_file = out_path / "taxonomy_balanced.parquet"
                    combined_df.write_parquet(output_file, compression="zstd")
                    logger.info(
                        f"Saved {len(combined_df)} taxonomy balanced samples to {output_file}"
                    )

            end_time = time.perf_counter()
            logger.info(
                f"Finished processing month {month} in {end_time - start_time:.2f} seconds."
            )


def main():
    """Main entry point for the sampler command-line interface.

    Parses command-line arguments and executes the sampling process with
    the specified configuration.
    """
    parser = argparse.ArgumentParser(description="Sample network flow data.")
    parser.add_argument(
        "--data-path",
        type=str,
        default=Path("data/flows"),
        help="Path to the data directory.",
    )
    parser.add_argument(
        "--sample-path",
        type=str,
        default=Path("data/samples"),
        help="Path to the sample output directory.",
    )
    parser.add_argument(
        "-y", "--year", type=int, required=True, help="Year to sample."
    )
    parser.add_argument(
        "-n",
        "--num-samples",
        type=int,
        default=100,
        help="Number of samples per taxonomy.",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for sampling."
    )
    parser.add_argument(
        "--balance-malicious-benign",
        action="store_true",
        help="Balance malicious and benign samples.",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging."
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled.")

    logger.debug(f"Arguments: {args}")

    sampler = Sampler(
        data_path=args.data_path,
        sample_path=args.sample_path,
        year=args.year,
        n=args.num_samples,
        seed=args.seed,
        balance_malicious_benign=args.balance_malicious_benign,
    )
    sampler.sample()


if __name__ == "__main__":
    main()
