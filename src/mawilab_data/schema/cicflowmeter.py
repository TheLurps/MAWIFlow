from typing import Dict, List, Any
from dataclasses import dataclass
from polars import String, Int64, Float64, Datetime, UInt16, UInt8


class CicFlowMeterSchema:
    """Schema definition for CICFlowMeter data.

    This class contains the field definitions and SQL casting expressions
    for the CICFlowMeter data format.
    """

    @dataclass
    class Field:
        """Represents a field in the CICFlowMeter schema."""

        name: str
        data_type: str
        description: str = ""

    # Field definitions with their DuckDB data types
    FIELDS: List[Field] = [
        Field("Flow ID", "VARCHAR", "Unique identifier for the flow"),
        Field("Src IP", "VARCHAR", "Source IP address"),
        Field("Src Port", "BIGINT", "Source port number"),
        Field("Dst IP", "VARCHAR", "Destination IP address"),
        Field("Dst Port", "BIGINT", "Destination port number"),
        Field("Protocol", "BIGINT", "Protocol number"),
        Field("Timestamp", "TIMESTAMP", "Start time of the flow"),
        Field(
            "Flow Duration", "BIGINT", "Duration of the flow in microseconds"
        ),
        Field(
            "Total Fwd Packet",
            "BIGINT",
            "Total packets in the forward direction",
        ),
        Field(
            "Total Bwd packets",
            "BIGINT",
            "Total packets in the backward direction",
        ),
        Field(
            "Total Length of Fwd Packet",
            "DOUBLE",
            "Total size of packet in forward direction",
        ),
        Field(
            "Total Length of Bwd Packet",
            "DOUBLE",
            "Total size of packet in backward direction",
        ),
        Field(
            "Fwd Packet Length Max",
            "DOUBLE",
            "Maximum size of packet in forward direction",
        ),
        Field(
            "Fwd Packet Length Min",
            "DOUBLE",
            "Minimum size of packet in forward direction",
        ),
        Field(
            "Fwd Packet Length Mean",
            "DOUBLE",
            "Mean size of packet in forward direction",
        ),
        Field(
            "Fwd Packet Length Std",
            "DOUBLE",
            "Standard deviation size of packet in forward direction",
        ),
        Field(
            "Bwd Packet Length Max",
            "DOUBLE",
            "Maximum size of packet in backward direction",
        ),
        Field(
            "Bwd Packet Length Min",
            "DOUBLE",
            "Minimum size of packet in backward direction",
        ),
        Field(
            "Bwd Packet Length Mean",
            "DOUBLE",
            "Mean size of packet in backward direction",
        ),
        Field(
            "Bwd Packet Length Std",
            "DOUBLE",
            "Standard deviation size of packet in backward direction",
        ),
        Field("Flow Bytes/s", "DOUBLE", "Number of bytes per second"),
        Field("Flow Packets/s", "DOUBLE", "Number of packets per second"),
        Field("Flow IAT Mean", "DOUBLE", "Mean inter-arrival time of packets"),
        Field(
            "Flow IAT Std",
            "DOUBLE",
            "Standard deviation inter-arrival time of packets",
        ),
        Field(
            "Flow IAT Max", "DOUBLE", "Maximum inter-arrival time of packets"
        ),
        Field(
            "Flow IAT Min", "DOUBLE", "Minimum inter-arrival time of packets"
        ),
        Field(
            "Fwd IAT Total",
            "DOUBLE",
            "Total inter-arrival time of packets in forward direction",
        ),
        Field(
            "Fwd IAT Mean",
            "DOUBLE",
            "Mean inter-arrival time of packets in forward direction",
        ),
        Field(
            "Fwd IAT Std",
            "DOUBLE",
            "Standard deviation inter-arrival time of packets in forward direction",
        ),
        Field(
            "Fwd IAT Max",
            "DOUBLE",
            "Maximum inter-arrival time of packets in forward direction",
        ),
        Field(
            "Fwd IAT Min",
            "DOUBLE",
            "Minimum inter-arrival time of packets in forward direction",
        ),
        Field(
            "Bwd IAT Total",
            "DOUBLE",
            "Total inter-arrival time of packets in backward direction",
        ),
        Field(
            "Bwd IAT Mean",
            "DOUBLE",
            "Mean inter-arrival time of packets in backward direction",
        ),
        Field(
            "Bwd IAT Std",
            "DOUBLE",
            "Standard deviation inter-arrival time of packets in backward direction",
        ),
        Field(
            "Bwd IAT Max",
            "DOUBLE",
            "Maximum inter-arrival time of packets in backward direction",
        ),
        Field(
            "Bwd IAT Min",
            "DOUBLE",
            "Minimum inter-arrival time of packets in backward direction",
        ),
        Field(
            "Fwd PSH Flags",
            "BIGINT",
            "Number of packets with PUSH flag in forward direction",
        ),
        Field(
            "Bwd PSH Flags",
            "BIGINT",
            "Number of packets with PUSH flag in backward direction",
        ),
        Field(
            "Fwd URG Flags",
            "BIGINT",
            "Number of packets with URGENT flag in forward direction",
        ),
        Field(
            "Bwd URG Flags",
            "BIGINT",
            "Number of packets with URGENT flag in backward direction",
        ),
        Field(
            "Fwd Header Length",
            "BIGINT",
            "Total bytes used for headers in forward direction",
        ),
        Field(
            "Bwd Header Length",
            "BIGINT",
            "Total bytes used for headers in backward direction",
        ),
        Field(
            "Fwd Packets/s", "DOUBLE", "Number of forward packets per second"
        ),
        Field(
            "Bwd Packets/s", "DOUBLE", "Number of backward packets per second"
        ),
        Field("Packet Length Min", "DOUBLE", "Minimum length of a packet"),
        Field("Packet Length Max", "DOUBLE", "Maximum length of a packet"),
        Field("Packet Length Mean", "DOUBLE", "Mean length of a packet"),
        Field(
            "Packet Length Std",
            "DOUBLE",
            "Standard deviation length of a packet",
        ),
        Field("Packet Length Variance", "DOUBLE", "Variance of packet lengths"),
        Field("FIN Flag Count", "BIGINT", "Number of packets with FIN flag"),
        Field("SYN Flag Count", "BIGINT", "Number of packets with SYN flag"),
        Field("RST Flag Count", "BIGINT", "Number of packets with RST flag"),
        Field("PSH Flag Count", "BIGINT", "Number of packets with PSH flag"),
        Field("ACK Flag Count", "BIGINT", "Number of packets with ACK flag"),
        Field("URG Flag Count", "BIGINT", "Number of packets with URG flag"),
        Field("CWR Flag Count", "BIGINT", "Number of packets with CWR flag"),
        Field("ECE Flag Count", "BIGINT", "Number of packets with ECE flag"),
        Field("Down/Up Ratio", "DOUBLE", "Download and upload ratio"),
        Field("Average Packet Size", "DOUBLE", "Average size of packet"),
        Field(
            "Fwd Segment Size Avg",
            "DOUBLE",
            "Average size observed in the forward direction",
        ),
        Field(
            "Bwd Segment Size Avg",
            "DOUBLE",
            "Average size observed in the backward direction",
        ),
        Field(
            "Fwd Bytes/Bulk Avg",
            "BIGINT",
            "Average number of bytes bulk rate in the forward direction",
        ),
        Field(
            "Fwd Packet/Bulk Avg",
            "BIGINT",
            "Average number of packets bulk rate in the forward direction",
        ),
        Field(
            "Fwd Bulk Rate Avg",
            "BIGINT",
            "Average number of bulk rate in the forward direction",
        ),
        Field(
            "Bwd Bytes/Bulk Avg",
            "BIGINT",
            "Average number of bytes bulk rate in the backward direction",
        ),
        Field(
            "Bwd Packet/Bulk Avg",
            "BIGINT",
            "Average number of packets bulk rate in the backward direction",
        ),
        Field(
            "Bwd Bulk Rate Avg",
            "BIGINT",
            "Average number of bulk rate in the backward direction",
        ),
        Field(
            "Subflow Fwd Packets",
            "BIGINT",
            "The average number of packets in a sub flow in the forward direction",
        ),
        Field(
            "Subflow Fwd Bytes",
            "BIGINT",
            "The average number of bytes in a sub flow in the forward direction",
        ),
        Field(
            "Subflow Bwd Packets",
            "BIGINT",
            "The average number of packets in a sub flow in the backward direction",
        ),
        Field(
            "Subflow Bwd Bytes",
            "BIGINT",
            "The average number of bytes in a sub flow in the backward direction",
        ),
        Field(
            "FWD Init Win Bytes",
            "BIGINT",
            "The total number of bytes sent in initial window in the forward direction",
        ),
        Field(
            "Bwd Init Win Bytes",
            "BIGINT",
            "The total number of bytes sent in initial window in the backward direction",
        ),
        Field(
            "Fwd Act Data Pkts",
            "BIGINT",
            "Count of packets with at least 1 byte of TCP data payload in the forward direction",
        ),
        Field(
            "Fwd Seg Size Min",
            "BIGINT",
            "Minimum segment size observed in the forward direction",
        ),
        Field(
            "Active Mean",
            "DOUBLE",
            "Mean time a flow was active before becoming idle",
        ),
        Field(
            "Active Std",
            "DOUBLE",
            "Standard deviation time a flow was active before becoming idle",
        ),
        Field(
            "Active Max",
            "DOUBLE",
            "Maximum time a flow was active before becoming idle",
        ),
        Field(
            "Active Min",
            "DOUBLE",
            "Minimum time a flow was active before becoming idle",
        ),
        Field(
            "Idle Mean",
            "DOUBLE",
            "Mean time a flow was idle before becoming active",
        ),
        Field(
            "Idle Std",
            "DOUBLE",
            "Standard deviation time a flow was idle before becoming active",
        ),
        Field(
            "Idle Max",
            "DOUBLE",
            "Maximum time a flow was idle before becoming active",
        ),
        Field(
            "Idle Min",
            "DOUBLE",
            "Minimum time a flow was idle before becoming active",
        ),
        Field("Label", "VARCHAR", "Class label for the flow"),
        Field("filename", "VARCHAR", "Source filename"),
        Field("Anomaly ID", "USMALLINT", "ID of anomaly, if any"),
        Field("Taxonomy", "VARCHAR", "Taxonomy classification of the anomaly"),
        Field("Heuristic", "DOUBLE", "Heuristic score"),
        Field(
            "Detectors",
            "UTINYINT",
            "Number of detectors that identified the anomaly",
        ),
        Field("Distance", "DOUBLE", "Distance metric"),
    ]

    @classmethod
    def get_field_names(cls) -> List[str]:
        """Return a list of all field names in the schema."""
        return [field.name for field in cls.FIELDS]

    @classmethod
    def get_field_dict(cls) -> Dict[str, Field]:
        """Return a dictionary mapping field names to Field objects."""
        return {field.name: field for field in cls.FIELDS}

    @classmethod
    def get_sql_cast_expressions(cls, table_alias: str = "f") -> Dict[str, str]:
        """
        Generate SQL CAST expressions for each field using the specified table alias.

        Args:
            table_alias: The table alias to use in the SQL expressions

        Returns:
            Dictionary mapping field names to their SQL CAST expressions
        """
        cast_expressions = {}
        for field in cls.FIELDS:
            # Special case for the Anomaly ID field which is already cast in the SQL query
            if field.name == "Anomaly ID" and table_alias == "f":
                cast_expressions[field.name] = f'{table_alias}."{field.name}"'
            # Special cases for annotation fields which come from a different table
            elif (
                field.name in ["Taxonomy", "Heuristic", "Detectors", "Distance"]
                and table_alias == "f"
            ):
                column_name = field.name.lower().replace(" ", "_")
                cast_expressions[field.name] = (
                    f"CAST(a.{column_name} AS {field.data_type})"
                )
            else:
                cast_expressions[field.name] = (
                    f'CAST({table_alias}."{field.name}" AS {field.data_type})'
                )

        return cast_expressions

    @classmethod
    def get_formatted_sql_cast_expressions(cls, table_alias: str = "f") -> str:
        """
        Return a formatted string with all SQL CAST expressions.

        Useful for directly including in SQL queries.

        Args:
            table_alias: The table alias to use in the SQL expressions

        Returns:
            Formatted string with all SQL CAST expressions
        """
        cast_expressions = cls.get_sql_cast_expressions(table_alias)
        formatted_expressions = []

        for field_name, expr in cast_expressions.items():
            formatted_expressions.append(f'{expr} AS "{field_name}"')

        return ",\n                ".join(formatted_expressions)

    @classmethod
    def get_polars_dtypes(cls) -> Dict[str, Any]:
        """
        Return a dictionary mapping field names to their Polars data types.

        Returns:
            Dictionary mapping field names to their Polars data types
        """
        dtypes = {}

        for field in cls.FIELDS:
            if field.data_type == "VARCHAR":
                dtypes[field.name] = String
            elif field.data_type == "BIGINT":
                dtypes[field.name] = Int64
            elif field.data_type == "DOUBLE":
                dtypes[field.name] = Float64
            elif field.data_type == "TIMESTAMP":
                dtypes[field.name] = Datetime(time_unit="us", time_zone=None)
            elif field.data_type == "USMALLINT":
                dtypes[field.name] = UInt16
            elif field.data_type == "UTINYINT":
                dtypes[field.name] = UInt8

        return dtypes
