"""Ep 13 - The SQL / DataFrame tab: follow the rows.

Run: python3 labs/ep13_sql_follow_the_rows.py
Open: SQL / DataFrame tab -> click each query -> read "number of output rows" on every box, top to bottom.

The report joins 1 million orders to a product list. Query A uses a product list that accidentally
contains each product 3 times (a bad load). Query B removes the duplicates first.

Your turn:
  1. In query A, which box turns 1 million rows into more than 1 million? How many come out?
  2. In query B, dropDuplicates never appears by name. Which box is it, and how many rows leave it?
"""
from pyspark.sql import functions as F
from common import get_spark, label, keep_open, warm_up

spark = get_spark("lab13-sql-follow-the-rows", spark__sql__autoBroadcastJoinThreshold="-1")
orders = spark.range(0, 1_000_000).select(F.col("id").alias("order_id"), (F.col("id") % 1000).alias("product_id"),
                                          (F.rand(5) * 50).alias("amount"))
products = spark.range(0, 1000).select(F.col("id").alias("product_id"), F.concat(F.lit("p"), F.col("id")).alias("name"))
bad_products = products.union(products).union(products)          # each product 3 times
warm_up(spark)

label(spark, "A: revenue with the duplicated product list")
orders.join(bad_products, "product_id").agg(F.sum("amount")).collect()
label(spark, "B: revenue after dropDuplicates")
orders.join(bad_products.dropDuplicates(["product_id"]), "product_id").agg(F.sum("amount")).collect()
keep_open(spark)
