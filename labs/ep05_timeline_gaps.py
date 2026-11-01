"""Ep 5 - The Event Timeline: gaps mean the driver is busy and the executors are idle.

Run: python3 labs/ep05_timeline_gaps.py
Open: Jobs tab -> "Event Timeline" (top of the page).

Your turn:
  1. Find the two gaps between jobs. Which one is the Python loop, which one is toPandas()?
  2. Change N_LOOP to 60_000_000 and run again. What happens to the gap?
"""
import time
from pyspark.sql import functions as F
from common import get_spark, label, keep_open

N_LOOP = 20_000_000
spark = get_spark("lab05-timeline-gaps")
df = spark.range(0, 1_000_000).withColumn("x", F.rand(2))

label(spark, "job 1: count"); df.count()

t = time.time()                                   # driver-only work: executors sit idle
total = 0
for v in range(N_LOOP):
    total += (v * 7) % 13
print(f"python loop on the driver took {time.time()-t:.1f}s")

label(spark, "job 2: toPandas (200k rows to the driver)")
t = time.time(); pdf = df.limit(200_000).toPandas()
print(f"toPandas took {time.time()-t:.1f}s, then pandas work runs on the driver only")
pdf["y"] = pdf["x"].rolling(50).mean(); time.sleep(3)

label(spark, "job 3: count again"); df.filter("x > 0.5").count()
keep_open(spark)
