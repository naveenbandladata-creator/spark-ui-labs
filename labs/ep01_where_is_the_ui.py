"""Ep 1 - Where does the Spark UI live?

Run:   python3 labs/ep01_where_is_the_ui.py
Then:  open the link it prints (usually http://localhost:4040)
       and the cluster page http://localhost:8080

Your turn:
  1. Leave this running and start a SECOND copy in another terminal. Which port does the second UI use?
  2. Open the Environment tab. What does it say for spark.sql.shuffle.partitions? Why isn't it 200?
"""
from common import get_spark, label, keep_open

spark = get_spark("lab01-where-is-the-ui")
label(spark, "lab01: count 1 million numbers")
print("rows:", spark.range(1_000_000).count())
keep_open(spark)
