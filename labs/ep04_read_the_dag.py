"""Ep 4 - Reading the DAG: boxes, arrows, Exchange.

Run: python3 labs/ep04_read_the_dag.py
Open: Jobs -> the job -> "DAG Visualization", then the SQL / DataFrame tab -> the query.

Your turn:
  1. Count the Exchange boxes in the SQL tab graph. Each one is data moving between machines.
  2. Which box reports the biggest "number of output rows"? Which is smallest?
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open

spark = get_spark("lab04-read-the-dag", spark__sql__adaptive__enabled="false")
sales = spark.range(0, 3_000_000).select(F.col("id").alias("order_id"), (F.col("id") % 500).alias("store_id"),
                                         (F.rand(1) * 100).alias("amount"))
stores = spark.range(0, 500).select(F.col("id").alias("store_id"), (F.col("id") % 20).alias("region"))
q = (sales.filter("amount > 50")
          .join(stores.hint("merge"), "store_id")
          .groupBy("region").agg(F.sum("amount").alias("revenue")))
label(spark, "lab04: revenue by region")
q.write.mode("overwrite").format("noop").save()
print("Exchange boxes in the plan:", q._jdf.queryExecution().executedPlan().toString().count("Exchange"))
keep_open(spark)
