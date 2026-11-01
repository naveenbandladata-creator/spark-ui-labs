"""Ep 15 - The scan node: did Spark skip data?

Run: python3 labs/ep15_scan_node.py      (writes a small Parquet table to /tmp/spark-ui-labs/sales)
Open: SQL / DataFrame tab -> each query -> the "Scan parquet" box at the top. Read:
      number of files read, number of partitions read, size of files read.

The table is stored in one folder per month (partitionBy("month")), 12 months.
  A) filter on month          -> Spark opens only that month's folder
  B) filter on a function of the month column through a Python UDF -> Spark can't tell which folders to skip
  C) all months, but only the small "amount" column
  D) all months, the long "note" column
  Parquet stores each column separately, so C reads far fewer bytes from the SAME files than D.

Careful: "size of files read" on the scan box is the size of the files Spark OPENED. It is the same for C and D.
The bytes actually read are in the Stages tab, "Input" column.

Your turn:
  1. How many partitions (folders) did A read? And B?
  2. C and D open the same files. Compare their "Input" in the Stages tab. How many times more did D read?
"""
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from common import get_spark, label, keep_open

PATH = "/tmp/spark-ui-labs/sales"
spark = get_spark("lab15-scan-node")
label(spark, "setup: write 12 monthly folders")
(spark.range(0, 3_000_000)
      .select((F.col("id") % 12 + 1).alias("month"), (F.col("id") % 5000).alias("store_id"),
              (F.rand(7) * 100).alias("amount"), F.sha2(F.col("id").cast("string"), 256).alias("note"))
      .write.mode("overwrite").partitionBy("month").parquet(PATH))

sales = spark.read.parquet(PATH)
is_march = F.udf(lambda m: m == 3, BooleanType())
label(spark, "A: filter month = 3")
sales.filter(F.col("month") == 3).agg(F.sum("amount")).collect()
label(spark, "B: same filter through a Python UDF")
sales.filter(is_march("month")).agg(F.sum("amount")).collect()
label(spark, "C: all months, only the amount column")
sales.agg(F.sum("amount")).collect()
label(spark, "D: all months, the long note column")
sales.agg(F.sum(F.length("note"))).collect()
keep_open(spark)
