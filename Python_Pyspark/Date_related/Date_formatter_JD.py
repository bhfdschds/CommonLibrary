"""Common functionality for outputs.
By: Joshua Day, NHS Digital
"""
import datetime as dt
import re
from typing import Any, List, Optional, Union
from pyspark.sql import DataFrame, SparkSession, functions as sf
from pyspark.sql.column import Column
from pyspark.sql.functions import col, lit  # pylint: disable=no-name-in-module
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    MapType,
    StringType,
    StructType,
    TimestampType,
)
​
def parse_datetime_col(column: Column, trim_fractional: bool = True) -> Column:
    """
    Parse a datetime column from a DataFrame. This should parse any reasonable
    ISO-like datetime.
    If `trim_fractional` is True, trim fractional seconds from the datetime
    before parsing. A Spark 2.X bug means that these are discarded anyway.
    """
    date_time_formats = [
        ("yyyyMMdd", "HHmmss"),  # True Neutral
        ("yyyy-MM-dd", "HH:mm:ss"),  # Lawful Good
        ("yyyyMMdd", "HH:mm:ss"),  # Chaotic Neutral
        ("yyyy-MM-dd", "HHmmss"),  # Chaotic Evil
    ]
    separators = ["'T'", "' '", ""]  # Literal T, literal space, no separator.
    # Parse timezone (e.g. '+00:00'), no TZ.
    tz_formats = ["X", ""]
    patterns = []
    for date_format, time_format in date_time_formats:
        for separator in separators:
            for tz_format in tz_formats:
                patterns.append(f"{date_format}{separator}{time_format}{tz_format}")
    str_column = sf.trim(column.cast(StringType()))  # pylint: disable=no-member
    if trim_fractional:
        str_column = sf.regexp_replace(
            str_column, r"(\.[0-9]+)(?=Z$|[+-][0-9]{2}:?[0-9]{2}$|$)", ""
        )
    str_column = (
        sf.when(
            str_column.rlike("[0-9]{16}"),
            sf.concat(
                sf.substring(str_column, 0, 14), lit("+"), sf.substring(str_column, 15, 2)
            ),
        )
        .when(
            str_column.rlike("[0-9]{8}T[0-9]{8}"),
            sf.concat(
                sf.substring(str_column, 0, 15), lit("+"), sf.substring(str_column, 16, 2)
            ),
        )
        .otherwise(str_column)
    )
    return sf.when(
        column.isNotNull() & (sf.length(str_column) >= lit(8)),
        sf.coalesce(
            sf.to_timestamp(str_column),
            *[sf.to_timestamp(str_column, pattern) for pattern in patterns],
            parse_date_col(column),
        ),
    )