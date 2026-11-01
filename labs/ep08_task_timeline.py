"""Ep 8 - The task timeline: where each second goes.

Run: python3 labs/ep08_task_timeline.py
Open: Stages -> each stage -> "Event Timeline" and "Show Additional Metrics" (tick Scheduler Delay,
Task Deserialization Time, Executor Computing Time).

Same data, same answer, two ways:
  A) 400 tiny tasks  -> most of each bar is overhead (scheduling, setup), not real work
  B) 4 normal tasks  -> the bars are almost all green (computing)

Your turn:
  1. In job A, what share of a typical task is Executor Computing Time?
  2. Which job finished first? By how much?
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open

spark = get_spark("lab08-task-timeline")
data = spark.range(0, 400_000).withColumn("x", F.sqrt("id"))
label(spark, "warm-up (ignore)")
data.repartition(4).write.mode("overwrite").format("noop").save()
label(spark, "A: 400 tiny tasks")
data.repartition(400).write.mode("overwrite").format("noop").save()
label(spark, "B: 4 normal tasks")
data.repartition(4).write.mode("overwrite").format("noop").save()
keep_open(spark)
