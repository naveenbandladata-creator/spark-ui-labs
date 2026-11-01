"""Ep 24 - Final exam: read a Spark UI you've never seen.

Run: python3 labs/ep24_final_exam.py
Three jobs from three different teams. Each one has ONE main problem. For each job, using only the UI:
  a) name the problem, b) give the screen and the number that proves it, c) suggest the fix.
Use the 5-minute checklist (answers/ep24.md, bottom) if you get stuck - but try without it first.

Answers: answers/ep24.md
"""
import time
from pyspark.sql import functions as F
from common import get_spark, label, keep_open, warm_up

spark = get_spark("lab24-final-exam", spark__sql__adaptive__enabled="false")
warm_up(spark)

label(spark, "Job 1: nightly export")
spark.conf.set("spark.sql.shuffle.partitions", "2")
(spark.range(0, 5_000_000).select(F.col("id"), F.sha2(F.col("id").cast("string"), 256).alias("payload"))
      .orderBy("payload").write.mode("overwrite").format("noop").save())
spark.conf.set("spark.sql.shuffle.partitions", "8")

events = (spark.range(0, 3_000_000).select((F.col("id") % 1000).alias("page"), (F.col("id") % 400_000).alias("v"))
          .groupBy("page").agg(F.countDistinct("v").alias("visitors")))
for k, metric in enumerate([F.sum, F.max, F.avg], 1):
    label(spark, f"Job 2: dashboard tile {k}")
    events.agg(metric("visitors")).collect()

label(spark, "Job 3: weekly KPI")
pdf = spark.range(0, 400_000).select((F.col("id") % 7).alias("weekday"), (F.rand(14) * 10).alias("value")).toPandas()
kpi = pdf.groupby("weekday")["value"].apply(lambda s: sorted(s)[len(s) // 2])
time.sleep(2)
label(spark, "Job 3: save weekly KPI")
spark.createDataFrame(kpi.reset_index()).write.mode("overwrite").format("noop").save()
print("weekly KPI rows:", len(kpi))
keep_open(spark)
