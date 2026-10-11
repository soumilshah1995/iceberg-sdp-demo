from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

# Spark 4.1 requires this. Do not build a new SparkSession in pipeline code.
spark = SparkSession.active()

# A directory, not one file. Each new CSV dropped here is processed on the next run.
RAW = "file:///Users/sshah/IdeaProjects/Learn/spark-dp-examples/lab1/data/raw"


@dp.table(format="iceberg")
def raw_customers() -> DataFrame:
    """Bronze. Append new CSV files to dev.lab1.raw_customers."""
    return (
        spark.readStream.format("csv")
        .option("header", True)
        .schema("name STRING, cust_id STRING, age STRING, city STRING, last_updated STRING")
        .load(RAW)
    )
