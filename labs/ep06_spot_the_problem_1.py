"""Ep 6 - Spot the Problem #1.

A teammate says: "My report is slow, but the data is tiny." Run it, then use ONLY the Spark UI
(Jobs tab, Event Timeline, Stages, DAG) to answer:

  Q1. How many jobs ran? How many did you expect for "one report"?
  Q2. Which line of code created most of them? (Hint: match job descriptions to the code.)
  Q3. Is the slow part on the executors or on the driver? What in the Event Timeline tells you?
  Q4. Rewrite the report so it runs in one or two jobs.

Answers: answers/ep06.md  (try first!)
"""
import time
from pyspark.sql import functions as F
from common import get_spark, keep_open

spark = get_spark("lab06-spot-the-problem")
orders = (spark.range(0, 2_000_000)
          .select((F.col("id") % 10).alias("region"), (F.rand(3) * 100).alias("amount"))
          .filter("amount > 5"))

report = {}
for region in range(10):
    part = orders.filter(F.col("region") == region)
    print("checking region", region, "rows:", part.count())          # "just for debugging"
    report[region] = part.agg(F.sum("amount")).collect()[0][0]
    time.sleep(0.5)                                                    # "format the row for the report"
print({k: round(v) for k, v in report.items()})
keep_open(spark)
