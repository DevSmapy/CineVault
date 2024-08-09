from pyspark.sql import SparkSession, DataFrame
from typing import Any, Dict, List
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


def process_multiple_pages(
    data: List[Dict[str, Any]], spark: SparkSession
) -> Any:  # movieListResult
    df = spark.createDataFrame(data)
    return df


def save_data(df: DataFrame, partition: List[str], path: str) -> None:
    df.write.partitionBy(partition).mode("overwrite").parquet(path)
