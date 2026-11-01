"""Ep 7 - Summary Metrics: compare Max with Median.

Run: python3 labs/ep07_summary_metrics.py
Open: Stages tab -> the stage with 8 tasks and a Shuffle Read -> "Summary Metrics for 8 Completed Tasks".
(AQE is off so the problem stays visible. Ep 19 shows how AQE helps.)

Your turn:
  1. In the Shuffle Read Size / Records row, divide Max by Median. How many times bigger is the biggest task?
  2. Change HOT_SHARE from 8 to 1 and run again. What happens to Max vs Median and to the stage time?
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open

HOT_SHARE = 8          # out of 10 rows, how many use the same customer id
spark = get_spark("lab07-summary-metrics", spark__sql__adaptive__enabled="false",
                  spark__sql__autoBroadcastJoinThreshold="-1")
orders = spark.range(0, 4_000_000).withColumn(
    "customer", F.when(F.col("id") % 10 < HOT_SHARE, F.lit(7)).otherwise(F.col("id") % 500))
customers = spark.range(0, 500).withColumnRenamed("id", "customer").withColumn("name", F.concat(F.lit("c"), F.col("customer")))
label(spark, "lab07: orders joined to customers")
orders.join(customers, "customer").write.mode("overwrite").format("noop").save()
keep_open(spark)
