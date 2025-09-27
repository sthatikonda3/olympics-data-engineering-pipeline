# Databricks notebook source
# MAGIC %md 
# MAGIC ## Silver Notebook

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading the NOCS Data

# COMMAND ----------

df = spark.read.format("csv")\
            .option("header", True)\
            .option("inferSchema", True)\
            .load("abfss://bronze@akhilolmpicsdata.dfs.core.windows.net/nocs")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping the columns**

# COMMAND ----------

df = df.drop("country")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('tag',split(col('tag'),'-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .option("path","abfss://silver@akhilolmpicsdata.dfs.core.windows.net/nocs")\
        .saveAsTable("olympics.silver.nocs")

# COMMAND ----------

