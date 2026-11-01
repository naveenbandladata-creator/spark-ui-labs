"""Ep 10 - Spill: when a task runs out of memory room and moves data to disk.

Run: python3 labs/ep10_spill.py
Open: Stages tab -> the sort stage of each job -> Spill (Memory) and Spill (Disk).

Same sort, two partition counts:
  A) 2 partitions  -> each task sorts ~half the data in 512 MB executors -> spills
  B) 32 partitions -> each task's slice fits in memory -> no spill

Your turn:
  1. How much did job A spill to disk? Is "Spill (Memory)" bigger or smaller than "Spill (Disk)"? Why?
  2. Compare the two jobs' durations.
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open

spark = get_spark("lab10-spill", spark__sql__adaptive__enabled="false")
data = spark.range(0, 6_000_000).withColumn("s", F.sha2(F.col("id").cast("string"), 256))
for name, parts in [("A: sort with 2 partitions", 2), ("B: sort with 32 partitions", 32)]:
    spark.conf.set("spark.sql.shuffle.partitions", str(parts))
    label(spark, name)
    data.orderBy("s").write.mode("overwrite").format("noop").save()
keep_open(spark)
