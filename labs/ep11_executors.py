"""Ep 11 - The Executors tab: who did the work, and what went wrong.

Run: python3 labs/ep11_executors.py      (Linux, macOS or WSL)
Open: the Executors tab while it runs, then again at the end.

What happens:
  Job A: one task fails on its first try, then Spark retries it and the job still succeeds.
  Job B: halfway through, the lab kills one executor process (like a machine dying).
         Spark marks it Dead, starts a replacement, and re-runs the lost tasks.

Your turn:
  1. Which executor shows "Failed Tasks" > 0 after job A?
  2. After job B, how many executors are listed as Dead? Did the job still succeed?
  3. Find the retried task in the stage's task table (look at the "Attempt" column).
"""
import os, signal, subprocess, threading, time
from pyspark import TaskContext
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from common import get_spark, label, keep_open

spark = get_spark("lab11-executors", spark__task__maxFailures="4")

def flaky(v):
    ctx = TaskContext.get()
    if ctx.partitionId() == 0 and ctx.attemptNumber() == 0:
        raise RuntimeError("simulated failure on the first attempt")
    return v
flaky_udf = F.udf(flaky, LongType())

label(spark, "A: one task fails once, then succeeds on retry")
spark.range(0, 100_000, numPartitions=4).select(flaky_udf("id")).write.mode("overwrite").format("noop").save()

APP_ID = spark.sparkContext.applicationId

def kill_one_executor(after):
    time.sleep(after)
    out = subprocess.run(["pgrep", "-f", f"CoarseGrainedExecutorBackend.*{APP_ID}"], capture_output=True, text=True).stdout.split()
    if out:
        os.kill(int(out[0]), signal.SIGKILL)
        print(f">>> killed executor process {out[0]} (simulating a dead machine)", flush=True)

label(spark, "B: an executor dies mid-job")
threading.Thread(target=kill_one_executor, args=(6,), daemon=True).start()
(spark.range(0, 3_000_000, numPartitions=12).withColumn("s", F.sha2(F.col("id").cast("string"), 256))
      .groupBy((F.col("id") % 100).alias("k")).agg(F.countDistinct("s")).write.mode("overwrite").format("noop").save())
print("job B finished")
keep_open(spark)
