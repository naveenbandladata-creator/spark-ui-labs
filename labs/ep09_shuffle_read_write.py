"""Ep 9 - Shuffle Read and Shuffle Write: how much data moved between machines.

Run: python3 labs/ep09_shuffle_read_write.py
Open: Stages tab, compare the Shuffle Write column of the two jobs.

Both jobs end with the same total per store. Job A first groups by store AND a long receipt text
(1.5 million different values), so nothing gets smaller before the shuffle. Job B keeps only the two
columns it needs, so each machine sends just one small subtotal per store.

Your turn:
  1. How many times more data did job A shuffle than job B?
  2. In the SQL tab, compare "shuffle records written" on the first Exchange of each query.
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open

spark = get_spark("lab09-shuffle-read-write", spark__sql__adaptive__enabled="false")
sales = (spark.range(0, 1_500_000)
         .select((F.col("id") % 200).alias("store"), (F.rand(4) * 100).alias("amount"),
                 F.sha2(F.col("id").cast("string"), 256).alias("receipt_text")))

label(spark, "A: group by store AND receipt_text, then sum")
(sales.groupBy("store", "receipt_text").agg(F.sum("amount").alias("amt"))
      .groupBy("store").agg(F.sum("amt")).write.mode("overwrite").format("noop").save())
label(spark, "B: keep only store + amount, then sum")
sales.select("store", "amount").groupBy("store").agg(F.sum("amount")).write.mode("overwrite").format("noop").save()
keep_open(spark)
