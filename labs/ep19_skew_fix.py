"""Ep 19 - Data skew: find it, then fix it two ways.

Run: python3 labs/ep19_skew_fix.py
Open: Stages tab -> each job's join stage -> Summary Metrics (Duration, Shuffle Read). Then the SQL tab.

The same skewed join as Ep 7 (80% of orders belong to one customer), three ways:
  A) AQE off            -> one straggler task does most of the work
  B) salting            -> the hot key is split into 64 sub-keys, spread over all 16 tasks
  C) AQE skew join      -> Spark splits the oversized partition by itself at runtime
NOTE: AQE only treats a partition as skewed when it is 5x the median AND over 256 MB (defaults).
These lab tables are small, so C lowers the size limit to 1 MB to show the feature. Real jobs keep the default.

Your turn:
  1. For A, B and C: Max vs Median of Shuffle Read Records in the join stage. Which fix evened things out most?
     (Durations on a laptop are noisy; record counts are not.)
  2. In C's SQL graph, find the box that reports "number of skewed partitions". What does it say?
  3. Salting changed the code AND copied the customer table 64 times. AQE changed neither.
     When would you still need salting? (Hint: Ep 12's skew was in a groupBy, not a join.)
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open, warm_up

SALT = 64     # more salt values than partitions, so hashing spreads them evenly
spark = get_spark("lab19-skew-fix", spark__sql__shuffle__partitions="16", spark__sql__adaptive__enabled="false", spark__sql__autoBroadcastJoinThreshold="-1")
warm_up(spark)
orders = spark.range(0, 4_000_000, numPartitions=16).withColumn(   # 16 input pieces: AQE can split a skewed partition at most into one piece per input task
   
    "customer", F.when(F.col("id") % 10 < 8, F.lit(7)).otherwise(F.col("id") % 500))
customers = spark.range(0, 500).withColumnRenamed("id", "customer").withColumn("name", F.concat(F.lit("c"), F.col("customer")))

label(spark, "A: skewed join, AQE off")
orders.join(customers, "customer").write.mode("overwrite").format("noop").save()

label(spark, "B: salted join")
salted_orders = orders.withColumn("salt", (F.rand(11) * SALT).cast("int"))
salted_customers = customers.crossJoin(spark.range(SALT).withColumnRenamed("id", "salt").withColumn("salt", F.col("salt").cast("int")))
salted_orders.join(salted_customers, ["customer", "salt"]).write.mode("overwrite").format("noop").save()

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1MB")   # demo only, default 256MB
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1MB")              # demo only, default 64MB
label(spark, "C: AQE skew join")
orders.join(customers, "customer").write.mode("overwrite").format("noop").save()
keep_open(spark)
