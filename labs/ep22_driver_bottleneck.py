"""Ep 22 - Driver bottlenecks and collect().

Run: python3 labs/ep22_driver_bottleneck.py
Open: Jobs tab -> Event Timeline (the gaps), then the Executors tab (driver row), and read the printout.

  A) collect() 300,000 rows to the driver, then loop over them in Python.
     The executors finish in seconds, then sit idle while the driver works alone (a gap in the timeline).
  B) collect() a result bigger than spark.driver.maxResultSize (set to 8 MB here; the default is 1 GB).
     Spark refuses it to protect the driver. Without that limit, a big enough collect() crashes the driver.
  C) The same work as A, done by Spark itself: no gap, the executors do it in parallel.

Your turn:
  1. How long is the gap after job A in the Event Timeline? What was the driver doing?
  2. What error message did B print? Which setting does it name?
  3. Rewrite your own most common collect() as a Spark aggregation or a write.
"""
import time
from pyspark.sql import functions as F
from common import get_spark, label, keep_open

spark = get_spark("lab22-driver-bottleneck", spark__driver__maxResultSize="8m")
orders = spark.range(0, 300_000).select(F.col("id"), (F.col("id") % 50).alias("country"), (F.rand(12) * 100).alias("amount"))

label(spark, "A: collect 300k rows, then a Python loop on the driver")
t = time.time(); rows = orders.collect(); print(f"A: collect took {time.time()-t:.1f}s")
t = time.time(); totals = {}
for r in rows:
    totals[r["country"]] = totals.get(r["country"], 0) + r["amount"] * 1.1
time.sleep(2)                                   # "...and format the report"
print(f"A: driver-only Python loop took {time.time()-t:.1f}s (executors idle)")

label(spark, "B: collect a result bigger than maxResultSize")
try:
    spark.range(0, 2_000_000).select(F.sha2(F.col("id").cast("string"), 256).alias("h")).collect()
except Exception as e:
    msg = str(e)
    i = msg.find("Total size of serialized results")
    print("B failed as expected:", msg[i:i + 160] if i >= 0 else msg[:160])

label(spark, "C: same as A, done by Spark")
t = time.time()
orders.groupBy("country").agg((F.sum("amount") * 1.1).alias("total")).collect()
print(f"C: Spark aggregation took {time.time()-t:.1f}s, and only 50 rows came back to the driver")
keep_open(spark)
