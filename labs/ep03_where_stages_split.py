"""Ep 3 - The Stages tab: a job splits wherever data must move between machines.

Run: python3 labs/ep03_where_stages_split.py   then open Jobs -> click each job -> look at its stages.
(AQE is switched off here so the stages are easy to read. Ep 16 turns it back on.)

Your turn:
  1. Before looking: how many stages will each query need? Then check.
  2. In the stage list, which stages have "Shuffle Write" and which have "Shuffle Read"?
"""
from pyspark.sql import functions as F
from common import get_spark, label, jobs_in, keep_open

spark = get_spark("lab03-where-stages-split", spark__sql__adaptive__enabled="false")
sc = spark.sparkContext
df = spark.range(0, 2_000_000).withColumn("k", F.col("id") % 100)
dims = spark.range(0, 100).withColumnRenamed("id", "k").withColumn("name", F.concat(F.lit("c"), F.col("k")))

queries = {
    "1: filter + select (no data moves)": lambda: df.filter("id % 2 = 0").select("k").write.mode("overwrite").format("noop").save(),
    "2: groupBy (data moves once)": lambda: df.groupBy("k").count().write.mode("overwrite").format("noop").save(),
    "3: join + groupBy": lambda: df.join(dims.hint("merge"), "k").groupBy("name").count().write.mode("overwrite").format("noop").save(),
}
for name, run in queries.items():
    label(spark, name); run()
for name in queries:
    stages = sum(len(sc.statusTracker().getJobInfo(j).stageIds) for j in jobs_in(spark, name))
    print(f"{name:<40} -> {stages} stage(s)")
keep_open(spark)
