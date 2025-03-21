# Databricks notebook source
import ipywidgets as widgets
from ipywidgets import interact

# Load a dataset
sparkDF = spark.read.csv("/databricks-datasets/bikeSharing/data-001/day.csv", header="true", inferSchema="true")

# In this code, `(bins=(3, 10)` defines an integer slider widget that allows values between 3 and 10.
@interact(bins=(3, 100))
def plot_histogram(bins):
  pdf = sparkDF.toPandas()
  pdf.hist(column='temp', bins=bins)


# COMMAND ----------

# MAGIC %md ##asdf

# COMMAND ----------

# MAGIC %md ## asdf

# COMMAND ----------

# MAGIC %sql select "Asdfsdfsdasd" as x, "asdf" as y

# COMMAND ----------

# MAGIC %md osdfsdf

# COMMAND ----------

# MAGIC %sql select explode(array_repeat(1,1001))

# COMMAND ----------

import time
time.sleep(60)

# COMMAND ----------

# MAGIC %sql select 1 as x, 1 as y
# MAGIC
# MAGIC
# MAGIC where 1=1

# COMMAND ----------

# MAGIC %sql select explode(array_repeat(1, 1001))

# COMMAND ----------

display(sql("""
select 1 as x -- asdfjljsd;
"""))

# COMMAND ----------

# MAGIC %md ## ignore all `"""`

# COMMAND ----------

# MAGIC %sql select "sss" "x" as x

# COMMAND ----------

_sqldf = spark.sql(r"""""")
display(_sqldf)

# COMMAND ----------

# MAGIC %md ## just escape all `"`
# MAGIC
# MAGIC can't work due to case 2, because we would be injecting `\` into raw strings

# COMMAND ----------

# MAGIC %md ### case 1 - `"` inside of string literal

# COMMAND ----------

# MAGIC %sql select '"""' as x

# COMMAND ----------

_sqldf = spark.sql(r"""select '\"\"\"' as x""")
display(_sqldf)

# COMMAND ----------

# MAGIC %md ### case 2 - `"` to express string literal

# COMMAND ----------

# MAGIC %sql select "sss" "x" as x

# COMMAND ----------

_sqldf = spark.sql(r"""select \"sss\"\"\" as x""")
display(_sqldf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select "\"\"\"" as x

# COMMAND ----------

_sqldf = spark.sql(r"""select "\"\"\"" as x""")
display(_sqldf)

# COMMAND ----------

# MAGIC %scala
# MAGIC s"""
# MAGIC _sqldf = spark.sql(r\"\"\"select "\\"\\"\\"" as x\"\"\")
# MAGIC display(_sqldf)
# MAGIC """

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GPSStaging_CPS.RowCounts_{0}""".format(Stream.lower())) WHERE Streamname='PPBR'

# COMMAND ----------

r"""select '\"' as x"""

# COMMAND ----------

len(r"""\"""")

# COMMAND ----------

from __future__ import print_function
from ipywidgets import interact, interactive, fixed, interact_manual
import ipywidgets as widgets

# COMMAND ----------

def f(x):
    return x

# COMMAND ----------

interact(f, x=10);

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql(s""" """)

# COMMAND ----------

display(sql(f"""select '""""' as x """))

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.readStream.format("rate").load()
# MAGIC val sq = df
# MAGIC   .writeStream
# MAGIC   .format("memory")
# MAGIC   .queryName("test")
# MAGIC   .start()
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.readStream.format("rate").load()
# MAGIC val sq = df
# MAGIC   .writeStream
# MAGIC   .format("memory")
# MAGIC   .queryName("test")
# MAGIC   .start()
# MAGIC display(df)

# COMMAND ----------

# MAGIC %sql select 1 --; select 2;

# COMMAND ----------

# MAGIC %sql select * from diamonds

# COMMAND ----------

# MAGIC %sql
# MAGIC select 1 /* asdf
# MAGIC
# MAGIC */ where 1=1

# COMMAND ----------

df = sql(r"""select regexp_replace(color, '\\+',  " ") from diamonds""")
display(df)

# COMMAND ----------

# MAGIC %sql select regexp_replace("asdf+", "\\+",  " ") -- in python notebook

# COMMAND ----------

# MAGIC %sql select regexp_replace("asdf+", "\\+",  " ")

# COMMAND ----------

# MAGIC %sql select "1"-"0"

# COMMAND ----------

sql("select 1")

# COMMAND ----------

# MAGIC %sql select explode(array_repeat(0, 1001))

# COMMAND ----------

displayHTML("xxx&#31;xxx")

# COMMAND ----------

# MAGIC %r
# MAGIC Sys.sleep(500/1000)
# MAGIC 1

# COMMAND ----------

# MAGIC %scala
# MAGIC Thread.sleep(500/1000)
# MAGIC 1

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

!pwd

# COMMAND ----------

spark.sql(r"{}".format)

# COMMAND ----------

# MAGIC %run ./asdfoiajsoidf

# COMMAND ----------

displayHTML("""<script>console.log('XXX', document.cookie)</script>""")

# COMMAND ----------

# MAGIC %python sql("select 1").show()

# COMMAND ----------

display(sql("select 1"))
sql("select 1").show()
display(sql("select 1"))

# COMMAND ----------

# MAGIC %sql select "\$1" as dollar

# COMMAND ----------

import time
time.sleep(60)

# COMMAND ----------

# MAGIC %sql select 1

# COMMAND ----------

# MAGIC %sql select 1

# COMMAND ----------

# MAGIC %sql select 1

# COMMAND ----------


