"""Ep 21 - The Python UDF cost.

Run: python3 labs/ep21_python_udf_cost.py      (needs pyarrow for the pandas UDF: pip install pyarrow)
Open: SQL / DataFrame tab. Compare durations, and find these boxes:
      BatchEvalPython (row-by-row Python UDF), ArrowEvalPython (pandas UDF), and neither for the built-in.

Same result three ways: clean up an email address (trim, lower-case, keep the domain).
  A) Python UDF  - every row is sent from Spark to a Python process and back, one at a time
  B) pandas UDF  - rows travel in big batches (Arrow), Python works on a whole column at once
  C) built-in    - Spark's own functions; the data never leaves Spark

Your turn:
  1. Rank A, B, C by duration. How many times slower is A than C?
  2. Which box name tells you, in someone else's job, that a Python UDF is slowing it down?
"""
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from common import get_spark, label, keep_open, warm_up

spark = get_spark("lab21-python-udf-cost")
warm_up(spark)
emails = spark.range(0, 6_000_000).select(F.concat(F.lit("  User"), F.col("id"), F.lit("@Example"),
                                                   (F.col("id") % 50).cast("string"), F.lit(".COM ")).alias("email"))

@F.udf(StringType())
def domain_udf(e):
    return e.strip().lower().split("@")[1]

@F.pandas_udf(StringType())
def domain_pandas(e: pd.Series) -> pd.Series:
    return e.str.strip().str.lower().str.split("@").str[1]

label(spark, "A: Python UDF")
emails.select(domain_udf("email").alias("d")).groupBy("d").count().write.mode("overwrite").format("noop").save()
label(spark, "B: pandas UDF")
emails.select(domain_pandas("email").alias("d")).groupBy("d").count().write.mode("overwrite").format("noop").save()
label(spark, "C: built-in functions")
emails.select(F.split(F.lower(F.trim("email")), "@").getItem(1).alias("d")).groupBy("d").count().write.mode("overwrite").format("noop").save()
keep_open(spark)
