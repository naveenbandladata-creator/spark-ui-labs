"""Ep 2 - The Jobs tab: one action is NOT always one job.

Run: python3 labs/ep02_one_action_many_jobs.py   then open the Jobs tab.
Each action below is labelled, so you can match it to its job(s).

Your turn:
  1. Which action created the most jobs? Look at the Jobs tab before reading the printout.
  2. Delete every label(...) line and run again. How readable is the Jobs tab now?
"""
from pyspark.sql import functions as F
from common import get_spark, label, jobs_in, keep_open

spark = get_spark("lab02-one-action-many-jobs")
big = spark.range(0, 2_000_000).withColumn("k", F.col("id") % 1000)
small = spark.range(0, 1000).withColumnRenamed("id", "k")

actions = {
    "A: count()": lambda: big.count(),
    "B: join with a small table, then count()": lambda: big.join(F.broadcast(small), "k").count(),
    "C: sort, then write": lambda: big.orderBy("k").write.mode("overwrite").format("noop").save(),
}
for name, run in actions.items():
    label(spark, name)
    run()
for name in actions:
    print(f"{name:<45} -> {len(jobs_in(spark, name))} job(s)")
keep_open(spark)
