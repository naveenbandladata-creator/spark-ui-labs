"""Ep 16 - AQE in the UI: Spark changing its plan while the job runs.

Run: python3 labs/ep16_aqe.py
Open: SQL / DataFrame tab. Each query's graph starts with "AdaptiveSparkPlan". Look for
      "isFinalPlan=true", the "AQEShuffleRead" boxes, and the join type.

Shuffle partitions are set to 200 here (the Spark default) on purpose.
  A) AQE off: the groupBy runs 200 tasks, most of them nearly empty.
  B) AQE on:  Spark sees the real data size after the shuffle and merges the tiny partitions.
  C) AQE on:  a join that LOOKS big when planned, but the filter leaves the store table tiny.
              Spark switches from sort-merge join to broadcast join at runtime.

Your turn:
  1. How many tasks did the groupBy stage run in A? In B?
  2. In C, which join type is in the graph? Now scroll down to "Details": compare the "Initial Plan" with the final one.
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open, warm_up

spark = get_spark("lab16-aqe", spark__sql__shuffle__partitions="200", spark__sql__adaptive__enabled="false",
                  spark__sql__autoBroadcastJoinThreshold="1048576")   # 1 MB, so the planner won't broadcast stores up front
warm_up(spark)
sales = spark.range(0, 3_000_000).select((F.col("id") % 50_000).alias("store_id"), (F.rand(8) * 100).alias("amount"))

label(spark, "A: AQE off, 200 shuffle partitions")
sales.groupBy("store_id").agg(F.sum("amount")).write.mode("overwrite").format("noop").save()

spark.conf.set("spark.sql.adaptive.enabled", "true")
label(spark, "B: AQE on, same query")
sales.groupBy("store_id").agg(F.sum("amount")).write.mode("overwrite").format("noop").save()

stores = spark.range(0, 50_000).select(F.col("id").alias("store_id"), (F.col("id") % 40).alias("region"),
                                       F.sha2(F.col("id").cast("string"), 256).alias("address"))
label(spark, "C: AQE on, join where the filter makes one side tiny")
(sales.join(stores.filter(F.col("region") == 7), "store_id")
      .groupBy("region").agg(F.sum("amount"), F.max("address")).write.mode("overwrite").format("noop").save())
keep_open(spark)
