"""Ep 20 - Too many or too few partitions.

Run: python3 labs/ep20_partition_count.py
Open: Stages tab. For each job compare: number of tasks, median task duration, Spill (Disk), total job time.

The same groupBy on the same data, with AQE off so the setting is used exactly as written:
  A) spark.sql.shuffle.partitions = 2000 -> thousands of tasks that each do almost nothing
  B) spark.sql.shuffle.partitions = 2    -> 2 huge tasks; they spill to disk and use only 2 cores
  C) spark.sql.shuffle.partitions = 16   -> a few tasks per core, no spill
  D) AQE on, starting from 2000          -> Spark merges the tiny partitions itself (target size 64 MB by default)

Your turn:
  1. Fill in a table: job, tasks, median task time, spill, total time. Which was fastest? Which was slowest, by how much?
  2. With 4 cores in this cluster, why is 2 partitions a bad choice even without the spill?
  3. AQE rescued A, but how many tasks did D end with, and did they spill? Its 64 MB target assumes real executors
     with several GB; ours have 512 MB. What does that tell you about trusting defaults?
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open, warm_up

spark = get_spark("lab20-partition-count", spark__sql__adaptive__enabled="false", spark__cores__max="4")
warm_up(spark)
data = (spark.range(0, 3_000_000, numPartitions=8)
        .select((F.col("id") % 400_000).alias("user"), F.sha2(F.col("id").cast("string"), 256).alias("event")))

for name, parts in [("A: 2000 partitions", 2000), ("B: 2 partitions", 2), ("C: 16 partitions", 16)]:
    spark.conf.set("spark.sql.shuffle.partitions", str(parts))
    label(spark, name)
    data.groupBy("user").agg(F.collect_set("event").alias("events")).select(F.size("events")).write.mode("overwrite").format("noop").save()

spark.conf.set("spark.sql.shuffle.partitions", "2000")
spark.conf.set("spark.sql.adaptive.enabled", "true")
label(spark, "D: AQE on, starting from 2000")
data.groupBy("user").agg(F.collect_set("event").alias("events")).select(F.size("events")).write.mode("overwrite").format("noop").save()
keep_open(spark)
