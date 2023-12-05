import argparse
from datetime import datetime
import logging
from pyspark.sql import SparkSession
import sys

__all__ = [
    "get_logical_date",
    "get_sample_reads",
    "get_write_in_test_mode",
    "parse_args",
    "setup_logging",
    "spark_session",
]


def get_logical_date(args):
    logical_date = datetime.strptime(args.logical_date, "%Y-%m-%d").date()
    return logical_date


# Python arg parse for booleans is pretty screwed up. We treat them as a string
# and expose these helper functions to handle str/bool confusion.
_TRUE_VALUES = ["True", "true", "T", "t", "1"]


def get_write_in_test_mode(args):
    return args.write_in_test_mode in _TRUE_VALUES


def get_sample_reads(args):
    return args.sample_reads in _TRUE_VALUES


def parse_args(
    default_write_in_test_mode: str = "False", default_sample_reads: str = "False"
):
    parser = argparse.ArgumentParser(description="logical_date_pyspark")
    _default_date = datetime.today().strftime("%Y-%m-%d")
    parser.add_argument(
        "--logical_date",
        type=str,
        default=_default_date,
        help="logical execution date in format of 'YYYY-mm-dd'.",
    )
    parser.add_argument(
        "--write_in_test_mode",
        type=str,
        default=default_write_in_test_mode,
        help="write output in a test database.",
    )
    parser.add_argument(
        "--sample_reads",
        type=str,
        default=default_sample_reads,
        help="read a sample of input data.",
    )
    args, _ = parser.parse_known_args()
    return args


def setup_logging():
    logger = logging.getLogger(__name__)
    # Setup logging
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logger


def spark_session(name: str) -> SparkSession:
    # Spark Context
    spark = (
        SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.network.timeout", "100001s")
        .config("spark.executor.heartbeatInterval", "100000s")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .appName(name)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark
