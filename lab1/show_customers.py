"""Print the Iceberg table this lab writes.

Run from lab1/ after spark-pipelines run:

    export SPARK_CONF_DIR="$PWD/conf"
    .venv/bin/python show_customers.py

Use the virtualenv interpreter. `python3` on this machine is aliased to Homebrew
PySpark, and that Spark build does not match the Iceberg 4.1 jar.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab1-show").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("dev.lab1.raw_customers")
spark.sql("SELECT * FROM dev.lab1.raw_customers ORDER BY cust_id").show(truncate=False)
print("dev.lab1.customers_by_age_group")
spark.sql("SELECT * FROM dev.lab1.customers_by_age_group ORDER BY age_group").show(truncate=False)
spark.stop()
