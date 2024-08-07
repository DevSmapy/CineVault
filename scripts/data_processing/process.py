from pyspark.sql import SparkSession
from typing import Any, Dict
from pyspark.sql.types import StructType, StructField, StringType


def str2dict(data: str) -> Dict[str, Any]:
    return eval(data)


def determine_schema(data: Dict[str, Any]) -> StructType:
    schema = StructType()
    for key, value in data.items():
        schema.add(StructField(key, StringType(), True))
    return schema


def process_ml_data(data: str, spark: SparkSession) -> Any:  # movieListResult
    data = str2dict(data)
    schema = determine_schema(data["movieListResult"]["movieList"][0])
    df = spark.createDataFrame(data["movieListResult"]["movieList"], schema=schema)
    return df
