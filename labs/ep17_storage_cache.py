"""Ep 17 - The Storage tab: is your cache actually used?

Run: python3 labs/ep17_storage_cache.py
Open: Storage tab (Fraction Cached, Size in Memory), then the SQL / DataFrame tab (look for InMemoryTableScan).

Three DataFrames call .cache():
  never   - cache() is called but no action ever runs on it. (cache is lazy: nothing is stored until an action)
  small   - cached, then used twice. The 2nd use reads from memory.
  big     - too big for the executors' memory. Only part of it fits; the rest is recomputed every time.

Your turn:
  1. Which of the three appear in the Storage tab? Why is one missing?
  2. What is the Fraction Cached of the big one?
  3. Both SQL graphs for "small" look the same: the original Range box is still drawn under InMemoryTableScan.
     Read the Range box's "number of output rows" in the 1st and 2nd use. What proves the 2nd use hit the cache?
  4. The Storage tab names the cached data with plan text. Bonus: use small.createOrReplaceTempView("small")
     and spark.catalog.cacheTable("small") instead of .cache(). What name does the Storage tab show now?
"""
from pyspark import StorageLevel
from pyspark.sql import functions as F
from common import get_spark, label, keep_open, warm_up

spark = get_spark("lab17-storage-cache")
warm_up(spark)
never = spark.range(0, 1_000_000).withColumn("x", F.col("id") * 3).cache()      # no action ever runs on it

small = (spark.range(0, 2_000_000).withColumn("bucket", F.col("id") % 100)
         .groupBy("bucket").agg(F.sum("id").alias("total")).cache())
label(spark, "small: 1st use (computes and fills the cache)")
small.agg(F.sum("total")).collect()
label(spark, "small: 2nd use (reads from the cache)")
small.agg(F.max("total")).collect()

big = (spark.range(0, 4_000_000, numPartitions=16)
       .withColumn("s", F.sha2(F.col("id").cast("string"), 512)).persist(StorageLevel.MEMORY_ONLY))
label(spark, "big: fill the cache (only part of it fits)")
big.count()
label(spark, "big: use it again")
big.filter(F.col("id") % 2 == 0).count()
keep_open(spark)
