"""Ep 12 - Spot the Problem #2.

"Our nightly customer summary is slow, and one task always finishes long after the others."
Run it, then use ONLY the Stages tab (Summary Metrics and the task table):

  Q1. Which stage is slow? How long does its slowest task take compared with the median task?
  Q2. Look at "Shuffle Read Size / Records". The record counts look almost even. The sizes don't.
      Give Max and Median for both. Which one tells the truth here?
  Q3. Why can the record counts look even when one task got far more data?
  Q4. Suggest two different fixes.

Answers: answers/ep12.md  (try first!)
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open

ROWS = 1_000_000     # bonus: try 3_000_000 and watch an executor die in the Executors tab (exit code 52 = out of memory)

spark = get_spark("lab12-spot-the-problem-2", spark__sql__adaptive__enabled="false")
orders = (spark.range(0, ROWS)
          .select(F.when(F.col("id") % 4 != 0, F.lit("guest")).otherwise(F.concat(F.lit("cust_"), (F.col("id") % 2000).cast("string"))).alias("customer"),
                  F.sha2(F.col("id").cast("string"), 256).alias("order_note")))
label(spark, "nightly customer summary")
(orders.groupBy("customer")
       .agg(F.count("*").alias("orders"), F.collect_list("order_note").alias("notes"))
       .select("customer", "orders", F.size("notes").alias("n_notes"))
       .write.mode("overwrite").format("noop").save())
keep_open(spark)
