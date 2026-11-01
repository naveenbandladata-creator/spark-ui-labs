"""Ep 23 - Case study: one slow job, three fixes.

Run: python3 labs/ep23_case_study.py
Then investigate the BEFORE job like a real one, in this order (the checklist from the series):
  Jobs tab -> which job is slow?   Stages tab -> which stage?   Summary Metrics -> Max vs Median?
  SQL tab  -> which boxes? (look for BatchEvalPython, the join type, Exchange count, row counts)

The report: revenue per product category for a web shop.
BEFORE has three problems hidden in it. AFTER fixes them. Same answer, compare the times.

Your turn:
  1. Find all three problems in BEFORE using only the UI, and write down the screen + number that proves each one.
  2. AFTER uses a broadcast join. Why does that also make the skew problem disappear?
"""
import time
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from common import get_spark, label, keep_open, warm_up

spark = get_spark("lab23-case-study", spark__sql__adaptive__enabled="false",
                  spark__sql__autoBroadcastJoinThreshold="-1",      # "someone turned broadcast off years ago"
                  spark__sql__shuffle__partitions="16")
warm_up(spark)
orders = spark.range(0, 3_000_000, numPartitions=8).select(
    F.when(F.col("id") % 10 < 7, F.lit(42)).otherwise(F.col("id") % 2000).alias("product_id"),   # one best-seller
    (F.rand(13) * 80).alias("price"),
    F.concat(F.lit("SKU-"), (F.col("id") % 2000).cast("string"), F.lit("-EU")).alias("sku"))
products = spark.range(0, 2000).select(F.col("id").alias("product_id"), (F.col("id") % 12).alias("category_id"))

@F.udf(StringType())
def region_of(sku):
    return sku.split("-")[2]

label(spark, "BEFORE")
t = time.time()
(orders.withColumn("region", region_of("sku")).filter(F.col("region") == "EU")
       .join(products, "product_id").groupBy("category_id").agg(F.sum("price").alias("revenue"))
       .write.mode("overwrite").format("noop").save())
print(f"BEFORE took {time.time()-t:.1f}s")

spark.conf.set("spark.sql.adaptive.enabled", "true")
label(spark, "AFTER")
t = time.time()
(orders.filter(F.split("sku", "-").getItem(2) == "EU")                 # fix 1: built-in instead of a Python UDF
       .join(F.broadcast(products), "product_id")                       # fix 2: broadcast the small table (no shuffle, no skew)
       .groupBy("category_id").agg(F.sum("price").alias("revenue"))     # fix 3: AQE on
       .write.mode("overwrite").format("noop").save())
print(f"AFTER took {time.time()-t:.1f}s")
keep_open(spark)
