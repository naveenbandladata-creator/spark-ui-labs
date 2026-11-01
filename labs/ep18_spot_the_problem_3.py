"""Ep 18 - Spot the Problem #3 (SQL / DataFrame tab).

"The March regional report reads a small table and joins a tiny lookup, but it takes forever."
Run it. Use ONLY the SQL / DataFrame tab (graph boxes and their numbers) and the Stages tab "Input" column:

  Q1. Scan box: how many partitions (month folders) were read? How many does a March report need?
  Q2. Which line of code stops Spark from skipping the other months?
  Q3. Join box: how many rows go in from the sales side, and how many come out? What does that tell you?
  Q4. Which join type did Spark use, and was it a good choice here?
  Q5. Rewrite the query to fix all three problems.

Answers: answers/ep18.md  (try first!)
"""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from common import get_spark, label, keep_open

PATH = "/tmp/spark-ui-labs/sales18"
spark = get_spark("lab18-spot-the-problem-3")
label(spark, "setup: write 12 monthly folders")
(spark.range(0, 2_400_000)
      .select((F.col("id") % 12 + 1).alias("month"), (F.col("id") % 300).alias("store_id"), (F.rand(9) * 100).alias("amount"))
      .write.mode("overwrite").partitionBy("month").parquet(PATH))
lookup = spark.range(0, 300).select(F.col("id").alias("store_id"), (F.col("id") % 6).alias("region"),
                                    F.sha2(F.col("id").cast("string"), 512).alias("manager_notes"))
lookup = lookup.union(lookup).union(lookup).union(lookup)          # loaded 4 times by a broken job

month_name = F.udf(lambda m: ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][m - 1], StringType())
sales = spark.read.parquet(PATH)
label(spark, "March regional report")
(sales.filter(month_name("month") == "Mar")
      .join(lookup, "store_id")
      .groupBy("region").agg(F.sum("amount").alias("revenue"), F.count("*").alias("rows"))
      .orderBy("region").show())
keep_open(spark)
