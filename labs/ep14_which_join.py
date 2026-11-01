"""Ep 14 - Which join did Spark pick? Broadcast vs sort-merge.

Run: python3 labs/ep14_which_join.py
Open: SQL / DataFrame tab. Compare the graphs of the two queries.

Same join, same answer:
  A) Spark is allowed to broadcast: the small table is copied to every executor (BroadcastHashJoin).
  B) Broadcasting is switched off: both sides are shuffled and sorted (SortMergeJoin).

Your turn:
  1. How many Exchange boxes does each query have? Which one has a BroadcastExchange instead?
  2. Compare the two durations on the SQL tab list page.
  3. Bonus: in B, put F.broadcast(stores) in the join. What does the graph show now?
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open, warm_up

spark = get_spark("lab14-which-join", spark__sql__adaptive__enabled="false")
sales = spark.range(0, 3_000_000).select((F.col("id") % 2000).alias("store_id"), (F.rand(6) * 100).alias("amount"))
stores = spark.range(0, 2000).select(F.col("id").alias("store_id"), (F.col("id") % 25).alias("region"))

warm_up(spark)
label(spark, "A: broadcast allowed (default 10 MB limit)")
sales.join(stores, "store_id").groupBy("region").agg(F.sum("amount")).write.mode("overwrite").format("noop").save()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
label(spark, "B: broadcast switched off")
sales.join(stores, "store_id").groupBy("region").agg(F.sum("amount")).write.mode("overwrite").format("noop").save()
keep_open(spark)
