import polars as pl
from pathlib import Path
from lxml import etree


class AnnotationReader:
    def __init__(self, annotation_file: Path):
        self.annotation_file = annotation_file

    def read(self):
        raise NotImplementedError("Subclasses should implement this method.")


class CsvAnnotationReader(AnnotationReader):
    def read(self) -> pl.DataFrame:
        """
        Read a annotations CSV file and return a Polars DataFrame.
        """

        # distance is missing
        if self.annotation_file.stem.endswith("_anomalous_suspicious"):
            return pl.read_csv(
                self.annotation_file,
                skip_rows=1,
                has_header=False,
                schema_overrides={
                    "anomaly_id": pl.UInt16,
                    "src_ip": pl.String,
                    "src_port": pl.UInt16,
                    "dst_ip": pl.String,
                    "dst_port": pl.UInt16,
                    "taxonomy": pl.String,
                    "distance": pl.Float64,
                    "num_detectors": pl.UInt8,
                    "label": pl.String,
                },
            )

        # anomalyID is missing
        elif self.annotation_file.stem.endswith("_notice"):
            return pl.read_csv(
                self.annotation_file,
                skip_rows=1,
                has_header=False,
                schema_overrides={
                    "src_ip": pl.String,
                    "src_port": pl.UInt16,
                    "dst_ip": pl.String,
                    "dst_port": pl.UInt16,
                    "taxonomy": pl.String,
                    "heuristic": pl.String,
                    "distance": pl.Float64,
                    "num_detectors": pl.UInt8,
                    "label": pl.String,
                },
            ).with_columns(
                [pl.arange(0, pl.len()).cast(pl.UInt16).alias("anomaly_id")]
            )

        else:
            raise ValueError(
                f"Unknown file type for {self.annotation_file}. Expected _anomalous_suspicious or _notice in filename."
            )


class XmlAnnotationReader(AnnotationReader):
    """
    Read a XML annotations file and return a Polars DataFrame.
    """

    def read(self) -> pl.DataFrame:
        # extract anomaly entries from xml
        tree = etree.parse(self.annotation_file)
        root = tree.getroot()
        nsmap = {
            "admd": "http://www.nict.go.jp/admd",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        }
        anomalies = root.findall(".//anomaly", namespaces=nsmap)

        # aggregate anomaly entries to dict
        data = []
        for index, anomaly in enumerate(anomalies):
            anomaly_type = anomaly.get("type")
            values = anomaly.get("value").split(",")
            detectors = values[3].split(" ")
            silices = anomaly.findall(".//slice/filter", namespaces=nsmap)
            start = anomaly.find(".//from").get("sec")
            stop = anomaly.find(".//to").get("sec")
            for filter in silices:
                data.append(
                    {
                        "anomaly_id": index,
                        "label": anomaly_type,
                        "distance_normal": values[0],
                        "distance_anomalous": values[1],
                        "heuristic": values[2],
                        "hough_sensitive": detectors[0],
                        "hough_optimal": detectors[1],
                        "hough_conservative": detectors[2],
                        "gamma_sensitive": detectors[3],
                        "gamma_optimal": detectors[4],
                        "gamma_conservative": detectors[5],
                        "kl_sensitive": detectors[6],
                        "kl_optimal": detectors[7],
                        "kl_conservative": detectors[8],
                        "pca_sensitive": detectors[9],
                        "pca_optimal": detectors[10],
                        "pca_conservative": detectors[11],
                        "taxonomy": values[4],
                        "start": start,
                        "stop": stop,
                        **filter.attrib,
                    }
                )

        # convert to dataframe
        df = pl.DataFrame(
            data,
            schema={
                "anomaly_id": pl.UInt16,
                "label": pl.String,
                "distance_normal": pl.Float64,
                "distance_anomalous": pl.Float64,
                "heuristic": pl.String,
                "hough_sensitive": pl.Boolean,
                "hough_optimal": pl.Boolean,
                "hough_conservative": pl.Boolean,
                "gamma_sensitive": pl.Boolean,
                "gamma_optimal": pl.Boolean,
                "gamma_conservative": pl.Boolean,
                "kl_sensitive": pl.Boolean,
                "kl_optimal": pl.Boolean,
                "kl_conservative": pl.Boolean,
                "pca_sensitive": pl.Boolean,
                "pca_optimal": pl.Boolean,
                "pca_conservative": pl.Boolean,
                "taxonomy": pl.String,
                "start": pl.UInt32,
                "stop": pl.UInt32,
                "src_ip": pl.String,
                "src_port": pl.UInt16,
                "dst_ip": pl.String,
                "dst_port": pl.UInt16,
                "proto": pl.String,
            },
        )

        # these values represent that filter is not set
        df = df.with_columns(
            [
                pl.when(pl.col("start").is_in([0, 2147483645]))
                .then(None)
                .otherwise(pl.col("start"))
                .alias("start"),
                pl.when(pl.col("stop").is_in([0, 2147483645]))
                .then(None)
                .otherwise(pl.col("stop"))
                .alias("stop"),
            ]
        )

        return df
