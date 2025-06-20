import duckdb
from datetime import datetime
import polars as pl
from mawilab_data.schema.cicflowmeter import CicFlowMeterSchema
import logging

logger = logging.getLogger(__name__)


class LocalSampler:
    def __init__(self):
        self.duckdb_conn = duckdb.connect()

    def create_sample(
        self,
        sample_size: int,
        seed: int = 42,
        start_date: datetime = datetime(2000, 1, 1),
        end_date: datetime = datetime(2025, 1, 1),
        data_path: str = "data/processed/v1.1/year=*/month=*/day=*/cicflowmeter_flows.parquet",
    ) -> pl.DataFrame:
        columns_sql = ",\n                ".join(
            [
                f'CAST("{field.name}" AS {field.data_type}) AS "{field.name}"'
                for field in CicFlowMeterSchema.FIELDS
            ]
        )

        query = f"""
            WITH all_data AS (
                SELECT
                    {columns_sql}
                FROM read_parquet(
                    '{data_path}',
                    hive_partitioning = 'true'
                )
                WHERE CAST(year AS BIGINT) >= {start_date.year} AND CAST(year AS BIGINT) <= {end_date.year}
                    AND CAST(month AS BIGINT) >= {start_date.month} AND CAST(month AS BIGINT) <= {end_date.month}
                    AND CAST(day AS BIGINT) >= {start_date.day} AND CAST(day AS BIGINT) <= {end_date.day}
            )
            SELECT * FROM all_data USING SAMPLE reservoir({sample_size} ROWS) REPEATABLE ({seed})
            ORDER BY Timestamp;
        """

        logger.debug(f"Executing query: {query}")
        df = self.duckdb_conn.execute(query).pl()

        return df.select(
            [
                pl.col(col).cast(dtype)
                for col, dtype in CicFlowMeterSchema.get_polars_dtypes().items()
            ]
        )
